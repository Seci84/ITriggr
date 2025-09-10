import os
import re
import json
import time
import traceback
from collections import defaultdict
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from firebase_admin import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

from common import init_db, log_event, sim_prefix, normalize, sha256, to_epoch, now_epoch
from fetch_news import fetch_newsapi, fetch_rss
from process_articles import fetch_contents, build_actionability, save_filtered
from rag import augment_with_wiki
import asyncio

# === LangChain / LangSmith ===
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser, JsonOutputParser
from langsmith import Client

# --- OpenAI 사용 여부 ---
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
USE_OPENAI = os.getenv("USE_OPENAI", "False").lower() == "true"

_llm = None
if OPENAI_API_KEY and USE_OPENAI:
    try:
        _llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.2)
        print("✅ OpenAI client initialized successfully")
    except Exception as e:
        print(f"❌ OpenAI client init failed: {e}")
        traceback.print_exc()
        USE_OPENAI = False
else:
    print(f"USE_OPENAI = {USE_OPENAI}, OPENAI_API_KEY is {'set' if OPENAI_API_KEY else 'not set'}")

# === LangSmith tracing(옵션) ===
if os.getenv("LANGSMITH_API_KEY"):
    os.environ["LANGCHAIN_TRACING_V2"] = "true"
    os.environ["LANGCHAIN_API_KEY"] = os.getenv("LANGSMITH_API_KEY")
    os.environ.setdefault("LANGCHAIN_PROJECT", os.getenv("LANGSMITH_PROJECT", "news-pipeline"))

# === LangSmith Prompt IDs ===
PROMPT_IDS = {
    "title": "news-title:2025-09-04",
    "summary": "news-summary:2025-09-04",
    "bullets": "news-bullets:2025-09-08",
    "facts": "news-facts:2025-09-04",
    "talks_general": "talks-general:2025-09-04",
    "talks_entrepreneur": "talks-entrepreneur:2025-09-04",
    "talks_politician": "talks-politician:2025-09-04",
    "talks_investor": "talks-investor:2025-09-04",
    "actionability": "actionability:2025-09-09"  # process_articles.py와 통합
}

_ls = Client()
_str = StrOutputParser()
_json = JsonOutputParser()

def _hub(name: str):
    """LangSmith에서 프롬프트를 끌어오되, 태그 못 찾으면 :latest로 폴백"""
    pid = PROMPT_IDS[name]
    try:
        return _ls.pull_prompt(pid)
    except Exception:
        base = pid.split(":", 1)[0]
        print(f"[Hub] Falling back to latest for {base}")
        return _ls.pull_prompt(f"{base}:latest")

# --- 유틸 (기존 그대로 유지) ---
def safe_parse_json(content: str):
    try:
        return json.loads(content)
    except Exception:
        pass
    content2 = re.sub(r"^```(?:json)?\s*|\s*```$", "", content.strip())
    try:
        return json.loads(content2)
    except Exception:
        pass
    m = re.search(r"\{.*\}", content, flags=re.S)
    if m:
        return json.loads(m.group(0))
    raise ValueError(f"JSON parse failed. head={content[:120]!r}")

def fetch_content(url):
    """URL에서 기사 본문 추출 (process_articles.py와 통합)"""
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        paragraphs = soup.find_all("p")
        content = " ".join(p.get_text() for p in paragraphs if p.get_text().strip())
        return content[:1500]
    except Exception as e:
        print(f"Failed to fetch content from {url}: {e}")
        return "Content unavailable"

def load_recent_raw_groups(db, window_sec=6*60*60, prefix_bits=16,
                           exclude_domains=("nytimes.com", "nyti.ms")):
    def _is_excluded(url: str) -> bool:
        try:
            host = urlparse(url or "").netloc.lower()
            return any(host == d or host.endswith("." + d) for d in exclude_domains)
        except Exception:
            return False

    now = int(time.time())
    since = now - window_sec
    q = db.collection("raw_articles_v6").where(filter=FieldFilter("published_at", ">=", since))
    groups = defaultdict(list)

    total, skipped = 0, 0
    for d in q.stream():
        it = d.to_dict() or {}
        url = it.get("url", "")
        total += 1
        if _is_excluded(url):
            skipped += 1
            continue
        k = sim_prefix(it.get("simhash", ""), prefix_bits=prefix_bits)
        groups[k].append((d.id, it))

    print(f"Loaded {len(groups)} clusters (total={total}, skipped={skipped})")
    return groups

def already_generated(db, cluster_key: str) -> bool:
    try:
        snap = (db.collection("generated_articles_v6")
                .where(filter=FieldFilter("cluster_key", "==", cluster_key))
                .limit(1).get())
        return len(snap) > 0
    except Exception as e:
        print(f"already_generated check failed: {e}")
        return False

def make_payload_from_sources(items):
    """Fallback payload (LLM 비활성/오류 시)"""
    n = len(items)
    title = f"[Auto] {n} source{'s' if n > 1 else ''}"
    summary = "Template summary (LLM disabled)"
    bullets = ["Key point 1", "Key point 2", "Key point 3"]
    first = items[0][1] if items else {}
    facts = [{"text": first.get("title", ""), "evidence_url": first.get("url", "")}]
    talks = {"general": "", "entrepreneur": "", "politician": "", "investor": ""}
    return {"title": title, "summary": summary, "bullets": bullets, "facts": facts, "talks": talks}

# === LangSmith 프롬프트 실행 (build_with_hub_prompts 수정) ===
def build_with_hub_prompts(input_text: str, sources: list[str]) -> dict:
    """본문 전체(input_text)만 각 프롬프트의 입력으로 사용, facts에 sources 전달."""
    if not _llm:
        return None
    try:
        print(f"[DEBUG] Received input_text length: {len(input_text)}, sample: {input_text[:100]}...")

        # summary
        summary = (_llm | _str).invoke(_format_prompt(_hub("summary"), input=input_text)).strip()
        print(f"[DEBUG] Summary generated: {summary[:50]}...")

        # bullets (JSON 배열로 수신)
        bullets_raw = (_llm | _json).invoke(_format_prompt(_hub("bullets"), input=input_text))
        bullets = bullets_raw if isinstance(bullets_raw, list) and len(bullets_raw) == 3 else ["Key point 1", "Key point 2", "Key point 3"]
        print(f"[DEBUG] Bullets generated: {bullets}")

        # title
        title = (_llm | _str).invoke(_format_prompt(_hub("title"), input=input_text)).strip()
        print(f"[DEBUG] Title generated: {title}")

        # facts (JSON)
        facts = (_llm | _json).invoke(
            _format_prompt(_hub("facts"), input=input_text, sources="\n".join(sources))
        )
        print(f"[DEBUG] Facts generated: {facts}")

        # talks
        summary_text = summary
        bullets_block = "\n".join(f"- {b}" for b in bullets)
        tg = (_llm | _str).invoke(_format_prompt(_hub("talks_general"), input=input_text, summary=summary_text, bullets=bullets_block)).strip()
        te = (_llm | _str).invoke(_format_prompt(_hub("talks_entrepreneur"), input=input_text, summary=summary_text, bullets=bullets_block)).strip()
        tp = (_llm | _str).invoke(_format_prompt(_hub("talks_politician"), input=input_text, summary=summary_text, bullets=bullets_block)).strip()
        ti = (_llm | _str).invoke(_format_prompt(_hub("talks_investor"), input=input_text, summary=summary_text, bullets=bullets_block)).strip()

        return {
            "title": title,
            "summary": summary,
            "bullets": bullets,
            "facts": facts if isinstance(facts, list) else [],
            "talks": {"general": tg, "entrepreneur": te, "politician": tp, "investor": ti}
        }
    except Exception as e:
        print(f"[HubBuild] error: {e}, input_text: {input_text[:100]}...")
        return None

# === 메인 파이프라인 ===
def run_once():
    db = init_db()
    # 1. 기사 수집
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
        return

    # 2. raw_articles_v6 저장
    saved, skipped, updated = 0, 0, 0
    col_raw = db.collection("raw_articles_v6")
    for it in all_items:
        url = it["url"]
        doc_id = sha256(url)  # fetch_news.py와 동일
        doc_ref = col_raw.document(doc_id)
        snap = doc_ref.get()
        it["url_hash"] = doc_id
        it["simhash"] = sim_prefix(simhash(f"{it['title']} {it.get('content_hint', '')}"))
        it.setdefault("created_at", firestore.SERVER_TIMESTAMP)

        if snap.exists:
            doc_ref.set({
                "source": it["source"],
                "source_name": it["source_name"],
                "title": it["title"],
                "url": it["url"],
                "published_at": it["published_at"],
                "content_hint": it.get("content_hint", ""),
                "lang": it.get("lang", "en"),
                "simhash": it["simhash"],
                "url_hash": it["url_hash"],
                "updated_at": firestore.SERVER_TIMESTAMP,
            }, merge=True)
            updated += 1
        else:
            doc_ref.set(it)
            saved += 1
    log_event(db, "ingest_done", {"saved": saved, "updated": updated, "total": len(all_items)})
    print(f"saved={saved} updated={updated} total={len(all_items)}")

    # 3. actionable_articles 필터링 및 RAG
    asyncio.run(analyze_and_save(db, all_items))

    # 4. 클러스터 처리 (기존 로직 유지)
    groups = load_recent_raw_groups(db)
    created = 0

    for cluster_key, items in groups.items():
        if len(items) < 1 or already_generated(db, cluster_key):
            continue
    
        evidence_urls, combined_texts = [], []
        ts_min, ts_max = 10**12, 0
        for _id, it in items:
            url = it.get("url", "")
            title = it.get("title", "")
            content = fetch_content(url)
    
            # 🔹 위키 컨텍스트 생성
            try:
                aug = augment_with_wiki({
                    "content": content,
                    "content_hint": it.get("content_hint", "")
                })
                wiki_ctx = aug.get("wiki_context", "")
            except Exception:
                wiki_ctx = ""
    
            evidence_urls.append(url)
    
            # 🔹 위키 컨텍스트를 LLM 입력 텍스트에만 덧붙임(존재할 때만)
            if wiki_ctx:
                combined_texts.append(f"{title}\n{content}\n\n[WIKI]\n{wiki_ctx}")
            else:
                combined_texts.append(f"{title}\n{content}")
    
            ts = int(it.get("published_at", 0) or 0)
            ts_min, ts_max = min(ts_min, ts), max(ts_max, ts)


        payload, latency_ms, model_used = None, 0, "template"
        if USE_OPENAI:
            try:
                t0 = time.time()
                input_text = "\n\n".join(combined_texts)
                payload = build_with_hub_prompts(input_text, evidence_urls)
                latency_ms = int((time.time() - t0) * 1000)
                if payload and payload.get("summary") != "Template summary (LLM disabled)":
                    model_used = "langsmith:gpt-4o-mini"
                else:
                    payload = make_payload_from_sources(items)
            except Exception as e:
                print(f"[LangSmith path] error: {e}")
                payload = make_payload_from_sources(items)
        else:
            payload = make_payload_from_sources(items)

        if payload is None:
            payload = make_payload_from_sources(items)

        doc = {
            "cluster_key": cluster_key,
            "title": payload.get("title", ""),
            "summary": payload.get("summary", ""),
            "bullets": payload.get("bullets", []),
            "facts": payload.get("facts", []),
            "talks": payload.get("talks", {}),
            "evidence_urls": evidence_urls,
            "raw_refs": [x[0] for x in items],
            "published_window": {"start": ts_min, "end": ts_max},
            "model": model_used,
            "token_usage": {},  # LangSmith에서 추적
            "latency_ms": latency_ms,
            "schema_version": "talks_v1",
            "created_at": firestore.SERVER_TIMESTAMP,
        }
        try:
            db.collection("generated_articles_v6").add(doc)
            created += 1
            print(f"[OK] Generated {cluster_key}, total={created}")
        except Exception as e:
            print(f"[ERROR] Failed to save to Firestore for {cluster_key}: {e}")

    log_event(db, "generate_done_v4", {"created": created})
    print(f"Done. groups={len(groups)}, created={created}")

if __name__ == "__main__":
    run_once()
