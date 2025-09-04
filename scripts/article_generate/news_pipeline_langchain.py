# 실제 뉴스 가져와 요약하고 Firestore에 저장하는 메인 로직.
# 프롬프트를 직접 하드코딩하지 않음
# langsmith에서 프롬프트 불러오기 → 모델 실행 → firebase로 저장

import os
import re
import json
import time
import traceback
from collections import defaultdict
from firebase_admin import firestore
from common import init_db, log_event, sim_prefix
from google.cloud.firestore_v1.base_query import FieldFilter
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# === LangChain / LangSmith Hub ===
from langchain.prompts import load_prompt
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser, JsonOutputParser
from langchain import hub

# --- OpenAI 사용 여부 ---
client = None
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
USE_OPENAI = os.getenv("USE_OPENAI", "False").lower() == "true"

if OPENAI_API_KEY and USE_OPENAI:
    try:
        client = ChatOpenAI(model="gpt-4o-mini", temperature=0.2)
        print("✅ OpenAI client initialized successfully")
    except Exception as e:
        print(f"❌ OpenAI client init failed: {e}")
        traceback.print_exc()
        USE_OPENAI = False
else:
    print(f"USE_OPENAI = {USE_OPENAI}, OPENAI_API_KEY is {'set' if OPENAI_API_KEY else 'not set'}")

# === LangSmith tracing (옵션) ===
if os.getenv("LANGSMITH_API_KEY"):
    os.environ["LANGCHAIN_TRACING_V2"] = "true"
    os.environ["LANGCHAIN_API_KEY"] = os.getenv("LANGSMITH_API_KEY")
    os.environ.setdefault("LANGCHAIN_PROJECT", os.getenv("LANGSMITH_PROJECT", "news-pipeline"))

# === LangSmith Prompt IDs ===
PROMPT_IDS = {
    "title":             "Personal/news-title:2025-09-04",
    "summary":           "Personal/news-summary:2025-09-04",
    "bullets":           "Personal/news-bullets:2025-09-04",
    "facts":             "Personal/news-facts:2025-09-04",
    "talks_general":     "Personal/news-talks-general:2025-09-04",
    "talks_entrepreneur":"Personal/news-talks-entrepreneur:2025-09-04",
    "talks_politician":  "Personal/news-talks-politician:2025-09-04",
    "talks_investor":    "Personal/news-talks-investor:2025-09-04",
    "final":             "Personal/final-json:2025-09-04",
}

_llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.2) if (USE_OPENAI and OPENAI_API_KEY) else None
_str = StrOutputParser()
_json = JsonOutputParser()

def _hub(name: str):
    return hub.pull(PROMPT_IDS[name])

# --- 유틸 ---
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
    """URL에서 기사 본문 추출"""
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
    q = db.collection("raw_articles").where(filter=FieldFilter("published_at", ">=", since))
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
        snap = (db.collection("generated_articles_v4")
                  .where(filter=FieldFilter("cluster_key", "==", cluster_key))
                  .limit(1).get())
        return len(snap) > 0
    except Exception as e:
        print(f"already_generated check failed: {e}")
        return False

def make_payload_from_sources(items):
    """Fallback payload"""
    n = len(items)
    title = f"[Auto] {n} source{'s' if n > 1 else ''}"
    summary = "Template summary (LLM disabled)"
    bullets = ["Key point 1", "Key point 2", "Key point 3"]
    first = items[0][1] if items else {}
    facts = [{"text": first.get("title", ""), "evidence_url": first.get("url", "")}]
    talks = {"general":"", "entrepreneur":"", "politician":"", "investor":""}
    return {"title":title, "summary":summary, "bullets":bullets, "facts":facts, "talks":talks}

# === LangSmith 프롬프트 실행 ===
def build_with_hub_prompts(input_text: str, sources: list[str]) -> dict:
    if not _llm:
        return None
    try:
        # summary
        summary = (_llm | _str).invoke(_hub("summary").format(input=input_text))
        # bullets
        bullets_raw = (_llm | _str).invoke(_hub("bullets").format(input=input_text))
        bullets = [l.strip("•- \t") for l in bullets_raw.splitlines() if l.strip()][:3]
        while len(bullets) < 3: bullets.append("Additional key point")
        # title
        title = (_llm | _str).invoke(_hub("title").format(input=input_text))
        # facts
        facts = (_llm | _json).invoke(_hub("facts").format(input=input_text, sources="\n".join(sources)))
        # talks
        tg = (_llm | _str).invoke(_hub("talks_general").format(summary=summary, bullets="\n".join(bullets)))
        te = (_llm | _str).invoke(_hub("talks_entrepreneur").format(summary=summary, bullets="\n".join(bullets)))
        tp = (_llm | _str).invoke(_hub("talks_politician").format(summary=summary, bullets="\n".join(bullets)))
        ti = (_llm | _str).invoke(_hub("talks_investor").format(summary=summary, bullets="\n".join(bullets)))
        # 최종 JSON
        final_payload = (_llm | _json).invoke(
            _hub("final").format(
                title=title,
                summary=summary,
                bullets_json=json.dumps(bullets, ensure_ascii=False),
                facts_json=json.dumps(facts, ensure_ascii=False),
                talk_general=tg,
                talk_entrepreneur=te,
                talk_politician=tp,
                talk_investor=ti,
            )
        )
        return final_payload
    except Exception as e:
        print(f"[HubBuild] error: {e}")
        return None

# === 메인 파이프라인 ===
def run_once():
    db = init_db()
    groups = load_recent_raw_groups(db)
    created = 0

    for cluster_key, items in groups.items():
        if len(items) < 1:
            continue
        if already_generated(db, cluster_key):
            continue

        evidence_urls, combined_texts = [], []
        ts_min, ts_max = 10**12, 0
        for _id, it in items:
            url = it.get("url", "")
            title = it.get("title", "")
            content = fetch_content(url)
            evidence_urls.append(url)
            combined_texts.append(f"{title}\n{content}")
            ts = int(it.get("published_at", 0) or 0)
            ts_min, ts_max = min(ts_min, ts), max(ts_max, ts)

        payload, latency_ms, model_used = None, 0, "template"
        if USE_OPENAI:
            try:
                t0 = time.time()
                input_text = "\n\n".join(combined_texts)
                payload = build_with_hub_prompts(input_text, evidence_urls)
                latency_ms = int((time.time()-t0)*1000)
                if payload:
                    model_used = "hub:gpt-4o-mini"
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
            "token_usage": {},
            "latency_ms": latency_ms,
            "schema_version": "talks_v1",
            "created_at": firestore.SERVER_TIMESTAMP,
        }
        db.collection("generated_articles_v4").add(doc)
        created += 1
        print(f"[OK] Generated {cluster_key}, total={created}")

    log_event(db, "generate_done", {"created": created})
    print(f"Done. groups={len(groups)}, created={created}")

if __name__ == "__main__":
    run_once()
