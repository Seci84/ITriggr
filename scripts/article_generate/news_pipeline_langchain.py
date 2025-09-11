# fetch_news.run_ingest()로 통과분 리스트를 받아 메모리에서 바로 군집화
# 통과분이 비어 있으면 actionable_articles에서 최근 문서 읽어 군집화(폴백)
# 생성 결과는 **generated_articles_v6**에만 저장
# doc_id_from_url()을 기본 키로 사용, raw_refs에는 통과분의 url_hash들을 기록


# news_pipeline_langchain.py
import os
import re
import json
import time
import traceback
from collections import defaultdict
from urllib.parse import urlparse
from typing import List, Dict, Tuple

import requests
from bs4 import BeautifulSoup
from firebase_admin import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

from common import (
    init_db, log_event, sim_prefix, normalize, sha256, to_epoch, now_epoch,
    simhash, doc_id_from_url
)
from fetch_news import run_ingest  # ← 통과분 리스트를 돌려줌
from process_articles import analyze_and_save, _format_prompt  # format 유틸 재사용
from rag import augment_with_wiki
import asyncio

from common import sanitize_url

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
}

_ls = Client()
_str = StrOutputParser()
_json = JsonOutputParser()

def _hub(name: str):
    """LangSmith에서 프롬프트 로드, 실패 시 :latest 폴백"""
    pid = PROMPT_IDS[name]
    try:
        return _ls.pull_prompt(pid)
    except Exception:
        base = pid.split(":", 1)[0]
        print(f"[Hub] Falling back to latest for {base}")
        return _ls.pull_prompt(f"{base}:latest")

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
    url = sanitize_url(url)
    """간단 본문 추출(생성 입력 보강용)"""
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        paragraphs = soup.find_all("p")
        content = " ".join(p.get_text() for p in paragraphs if p.get_text().strip())
        return normalize(content)[:500] or "Content unavailable"
    except Exception as e:
        print(f"Failed to fetch content from {url}: {e}")
        return "Content unavailable"

# =========================
# 통과분 전용 군집화 로직
# =========================
def group_passed_items(passed_items: List[Dict], prefix_bits: int = 16) -> Dict[str, List[Tuple[str, Dict]]]:
    """
    통과분 리스트(메모리)만으로 simhash prefix 군집화.
    반환: {cluster_key: [(doc_id(url_hash), item_dict), ...], ...}
    """
    groups = defaultdict(list)
    for it in passed_items:
        # url_hash 키가 없을 수 있으므로 보강
        uid = it.get("url_hash") or doc_id_from_url(it.get("url", ""))
        if not it.get("simhash"):
            it["simhash"] = simhash(f"{it.get('title','')} {it.get('content_hint','')}")
        k = sim_prefix(it["simhash"], prefix_bits=prefix_bits)
        groups[k].append((uid, it))
    print(f"Loaded {len(groups)} clusters from passed_items (total={len(passed_items)})")
    return groups

def load_recent_actionable_groups(db, window_sec=6*60*60, prefix_bits=16):
    """
    폴백: actionable_articles에서 최근 문서만 읽어 군집화.
    """
    now = int(time.time())
    since = now - window_sec
    q = db.collection("actionable_articles").where(filter=FieldFilter("published_at", ">=", since))
    groups = defaultdict(list)
    total = 0
    for d in q.stream():
        it = d.to_dict() or {}
        total += 1
        k = sim_prefix(it.get("simhash", ""), prefix_bits=prefix_bits)
        uid = d.id or it.get("url_hash") or doc_id_from_url(it.get("url", ""))
        groups[k].append((uid, it))
    print(f"Loaded {len(groups)} clusters from actionable_articles (total={total})")
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

# === LangSmith 프롬프트 실행
def build_with_hub_prompts(input_text: str, sources: List[str]) -> dict:
    """본문 전체(input_text)만 각 프롬프트의 입력으로 사용, facts에 sources 전달."""
    if not _llm:
        return None
    try:
        # summary
        summary = (_llm | _str).invoke(_format_prompt(_hub("summary"), input=input_text)).strip()

        # bullets (JSON 배열로 수신)
        bullets_raw = (_llm | _json).invoke(_format_prompt(_hub("bullets"), input=input_text))
        bullets = bullets_raw if isinstance(bullets_raw, list) and len(bullets_raw) == 3 else ["Key point 1", "Key point 2", "Key point 3"]

        # title
        title = (_llm | _str).invoke(_format_prompt(_hub("title"), input=input_text)).strip()

        # facts (JSON)
        facts = (_llm | _json).invoke(
            _format_prompt(_hub("facts"), input=input_text, sources="\n".join(sources))
        )
        if not isinstance(facts, list):
            facts = []

        # talks
        bullets_block = "\n".join(f"- {b}" for b in bullets)
        tg = (_llm | _str).invoke(_format_prompt(_hub("talks_general"), input=input_text, summary=summary, bullets=bullets_block)).strip()
        te = (_llm | _str).invoke(_format_prompt(_hub("talks_entrepreneur"), input=input_text, summary=summary, bullets=bullets_block)).strip()
        tp = (_llm | _str).invoke(_format_prompt(_hub("talks_politician"), input=input_text, summary=summary, bullets=bullets_block)).strip()
        ti = (_llm | _str).invoke(_format_prompt(_hub("talks_investor"), input=input_text, summary=summary, bullets=bullets_block)).strip()

        return {
            "title": title,
            "summary": summary,
            "bullets": bullets,
            "facts": facts,
            "talks": {"general": tg, "entrepreneur": te, "politician": tp, "investor": ti}
        }
    except Exception as e:
        print(f"[HubBuild] error: {e}")
        return None

# =========================
# 메인 파이프라인
# =========================
def run_once():
    db = init_db()

    # 1) 수집 → 본문 크롤링/점수화 → 통과분 리스트 확보
    passed_items: List[Dict] = []
    try:
        passed_items = run_ingest()  # SAVE_RAW=False면 raw 저장 없이 통과분만 생성됨
    except Exception as e:
        log_event(db, "err_ingest", {"msg": str(e)})
        print(f"[Ingest error] {e}")

    # 2) 군집 대상 결정: 메모리 통과분 우선, 없으면 actionable_articles 폴백
    if passed_items:
        groups = group_passed_items(passed_items, prefix_bits=16)
    else:
        groups = load_recent_actionable_groups(db, window_sec=6*60*60, prefix_bits=16)

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

            # RAG 컨텍스트 (필요 시)
            try:
                aug = augment_with_wiki({
                    "content": content,
                    "content_hint": it.get("content_hint", "")
                })
                wiki_ctx = aug.get("wiki_context", "")
            except Exception:
                wiki_ctx = ""

            evidence_urls.append(url)
            combined_texts.append(
                f"{title}\n{content}" + (f"\n\n[WIKI]\n{wiki_ctx}" if wiki_ctx else "")
            )

            ts = int(it.get("published_at", 0) or 0)
            ts_min, ts_max = min(ts_min, ts), max(ts_max, ts)

        # 3) LLM 생성
        payload, latency_ms, model_used = None, 0, "template"
        input_text = "\n\n".join(combined_texts)

        if USE_OPENAI:
            try:
                t0 = time.time()
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

        # 4) 저장 (generated_articles_v6)
        doc = {
            "cluster_key": cluster_key,
            "title": payload.get("title", ""),
            "summary": payload.get("summary", ""),
            "bullets": payload.get("bullets", []),
            "facts": payload.get("facts", []),
            "talks": payload.get("talks", {}),
            "evidence_urls": evidence_urls,
            "raw_refs": [x[0] for x in items],  # url_hash / actionable document id
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

    log_event(db, "generate_done_v6", {"created": created})
    print(f"Done. groups={len(groups)}, created={created}")

if __name__ == "__main__":
    run_once()
