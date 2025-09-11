# 역할: NewsAPI(+옵션 RSS)에서 기사 수집 → (옵션) raw 저장 → LLM 점수화 호출 → 통과분만 반환
# (유사 기사들은 남겨둠 → 이후 단계에서 "통과분만" 묶어서 재구성)
# SAVE_RAW 환경변수로 raw_articles_v6 저장 여부를 제어(기본 False).
# process_articles.analyze_and_save()를 호출하여 통과분만 DB 저장하고, 통과분 리스트를 반환

import os, requests, feedparser
from typing import List, Dict, Any, Tuple, Union
from common import init_db, now_epoch, to_epoch, normalize, sha256, simhash, log_event, doc_id_from_url
from firebase_admin import firestore
from process_articles import analyze_and_save
import asyncio

NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
RSS_SOURCES = os.getenv("RSS_SOURCES", "")
SAVE_RAW = os.getenv("SAVE_RAW", "False").lower() == "true"  # ✅ raw 저장 토글(기본 False)

def fetch_newsapi() -> List[Dict[str, Any]]:
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

def fetch_rss() -> List[Dict[str, Any]]:
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

def save_raw(db, items: List[Dict[str, Any]]) -> Tuple[int, int, int]:
    """URL 기준 멱등 upsert. SAVE_RAW=True 일 때만 사용."""
    saved, skipped, updated = 0, 0, 0
    col = db.collection("raw_articles_v6")

    for it in items:
        url = it["url"]
        doc_id = doc_id_from_url(url)           # URL 해시 = 문서 ID (공통화)
        doc_ref = col.document(doc_id)
        snap = doc_ref.get()

        # 유사도 군집용 값 계산 (제목+요약 힌트)
        it["url_hash"] = sha256(url)            # 참고용 필드(쿼리/검증)
        it["simhash"] = simhash(f"{it['title']} {it.get('content_hint','')}")
        it.setdefault("created_at", firestore.SERVER_TIMESTAMP)

        if snap.exists:
            # 이미 있으면 최신 메타만 업데이트
            doc_ref.set({
                "source": it["source"],
                "source_name": it["source_name"],
                "title": it["title"],
                "url": it["url"],
                "published_at": it["published_at"],
                "content_hint": it.get("content_hint", ""),
                "lang": it.get("lang","en"),
                "simhash": it["simhash"],
                "url_hash": it["url_hash"],
                "updated_at": firestore.SERVER_TIMESTAMP,
            }, merge=True)
            updated += 1
        else:
            doc_ref.set(it)                     # 최초 저장
            saved += 1

    return saved, skipped, updated

def run_ingest() -> List[Dict[str, Any]]:
    """수집 → (옵션) raw 저장 → LLM 점수화 호출 → 통과분 리스트 반환."""
    db = init_db()
    all_items: List[Dict[str, Any]] = []

    # 1) 수집
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
        return []

    # 2) (옵션) raw 저장
    if SAVE_RAW:
        saved, skipped, updated = save_raw(db, all_items)
        log_event(db, "ingest_raw_done", {"saved": saved, "updated": updated, "total": len(all_items)})
        print(f"[raw] saved={saved} updated={updated} total={len(all_items)}")
    else:
        print("[raw] skipped (SAVE_RAW=False)")

    # 3) 본문 크롤링 + LLM 점수화 + 통과분 DB 저장 + 통과분 리스트 반환
    try:
        result = asyncio.run(analyze_and_save(db, all_items))
    except Exception as e:
        log_event(db, "err_filter", {"msg": str(e)})
        print(f"Filter error: {e}")
        return []

    # 하위 호환 처리:
    # - 신버전: analyze_and_save → List[Dict] (통과분 리스트)
    # - 구버전: analyze_and_save → Tuple(saved, skipped)
    passed_items: List[Dict[str, Any]] = []
    if isinstance(result, list):
        passed_items = result
        log_event(db, "filter_done_returned", {"passed_count": len(passed_items)})
        print(f"[filter] passed={len(passed_items)} (returned list)")
    elif isinstance(result, tuple) and len(result) == 2:
        saved_cnt, skipped_cnt = result
        log_event(db, "filter_done_legacy", {"saved": saved_cnt, "skipped": skipped_cnt})
        print(f"[filter] saved={saved_cnt} skipped={skipped_cnt} (legacy tuple)")
        # 구버전인 경우, 통과분 리스트는 알 수 없으므로 빈 리스트 반환
    else:
        print(f"[filter] Unexpected return from analyze_and_save: {type(result)}")

    return passed_items

if __name__ == "__main__":
    passed = run_ingest()
    print(f"Done. passed_count={len(passed)}")
