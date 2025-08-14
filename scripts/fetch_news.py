# 역할: NewsAPI(+옵션 RSS)에서 기사 수집 → URL 기준 완전 중복만 제거 → raw_articles 저장
# (유사 기사들은 남겨둠 → 다음 단계에서 묶어서 재구성)

import os, requests, feedparser
from common import init_db, now_epoch, to_epoch, normalize, sha256, simhash, log_event
from firebase_admin import firestore

NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
RSS_SOURCES = os.getenv("RSS_SOURCES", "")

def fetch_newsapi():
    if not NEWSAPI_KEY:
        return []
    url = "https://newsapi.org/v2/top-headlines"
    params = {
        "language": "en",     # 필요시 ko 등 추가
        "pageSize": 50,
        "apiKey": NEWSAPI_KEY
    }
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    out = []
    for a in data.get("articles", []):
        title = normalize(a.get("title"))
        url = a.get("url")
        if not title or not url:
            continue
        out.append({
            "source": "newsapi",
            "source_name": (a.get("source") or {}).get("name") or "newsapi",
            "title": title,
            "url": url,
            "published_at": to_epoch(a.get("publishedAt"), default=now_epoch()),
            "content_hint": normalize(a.get("description")),  # 전문 저장하지 않음
            "lang": "en",
        })
    return out

def fetch_rss():
    if not RSS_SOURCES.strip():
        return []
    out = []
    for u in [x.strip() for x in RSS_SOURCES.split(",") if x.strip()]:
        feed = feedparser.parse(u)
        src_name = (getattr(feed, "feed", {}) or {}).get("title", "rss")
        for e in getattr(feed, "entries", []):
            title = normalize(getattr(e, "title", ""))
            link = getattr(e, "link", None)
            if not title or not link:
                continue
            published = getattr(e, "published", "") or getattr(e, "updated", "")
            out.append({
                "source": "rss",
                "source_name": src_name,
                "title": title,
                "url": link,
                "published_at": to_epoch(published, default=now_epoch()),
                "content_hint": normalize(getattr(e, "summary", "")),
                "lang": "en",
            })
    return out

def save_raw(db, items):
    saved, skipped = 0, 0
    col = db.collection("raw_articles")
    for it in items:
        # URL 해시로 "완전 중복"만 차단
        url_hash = sha256(it["url"])
        doc_ref = col.document(url_hash)
        if doc_ref.get().exists:
            skipped += 1
            continue
        # 유사도 군집용 simhash(제목+요약 힌트)
        s = simhash(f"{it['title']} {it.get('content_hint','')}")
        it["url_hash"] = url_hash
        it["simhash"] = s
        it["created_at"] = firestore.SERVER_TIMESTAMP
        doc_ref.set(it)
        saved += 1
    return saved, skipped

if __name__ == "__main__":
    db = init_db()
    all_items = []
    try:
        all_items += fetch_newsapi()
    except Exception as e:
        log_event(db, "err_newsapi", {"msg": str(e)})
    try:
        all_items += fetch_rss()
    except Exception as e:
        log_event(db, "err_rss", {"msg": str(e)})

    if not all_items:
        log_event(db, "no_items", {})
        print("No items")
        raise SystemExit(0)

    saved, skipped = save_raw(db, all_items)
    log_event(db, "ingest_done", {"saved": saved, "skipped": skipped, "total": len(all_items)})
    print(f"saved={saved} skipped={skipped} total={len(all_items)}")
