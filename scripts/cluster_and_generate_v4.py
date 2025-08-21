import os
import re
import json
import time
import traceback
from collections import defaultdict
from firebase_admin import firestore
from common import init_db, log_event, sim_prefix
from openai.types.chat.completion_create_params import ResponseFormat
from openai import OpenAI
from google.cloud.firestore_v1.base_query import FieldFilter

# --- OpenAI 사용 여부 ---
client = None  # ✅ 항상 미리 정의
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
USE_OPENAI = os.getenv("USE_OPENAI", "False").lower() == "true"

if OPENAI_API_KEY and USE_OPENAI:
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)  # proxies 인자 제거
        print("✅ OpenAI client initialized successfully")
    except Exception as e:
        print(f"❌ OpenAI client init failed: {e}")
        print("Full stack trace:")
        traceback.print_exc()
        USE_OPENAI = False
else:
    print(f"USE_OPENAI = {USE_OPENAI}, OPENAI_API_KEY is {'set' if OPENAI_API_KEY else 'not set'}")

# --- LLM 프롬프트: JSON만! (주석/코드펜스 금지) ---
PROMPT = """You are a news rewrite assistant.
Return ONLY a single JSON object. No code fences, no explanations, no comments.

Required JSON shape:
{{
  "title": "string",
  "summary": "string",
  "bullets": ["string", "string", "string"],
  "facts": [{{"text":"string","evidence_url":"string"}}],
  "actions": {{
    "stock":[{{"action":"","assumptions":"","risk":"","alternative":""}}],
    "futures":[{{"action":"","assumptions":"","risk":"","alternative":""}}],
    "biz":[{{"action":"","assumptions":"","risk":"","alternative":""}}]
  }}
}}

Rules:
- Use available sources (one or more). Cite at least 1 item in "facts" with evidence_url chosen from the given Sources list.
- Cautious, factual tone. No guarantees/advice.
- If mostly Korean sources, write Korean; otherwise English.

Sources:
{sources}
"""

def safe_parse_json(content: str):
    """LLM 응답에서 JSON만 안전하게 추출."""
    try:
        return json.loads(content)
    except Exception:
        pass
    content2 = re.sub(r"^```(?:json)?\s*|\s*```$", "", content.strip(), flags=re.I | re.M)
    try:
        return json.loads(content2)
    except Exception:
        pass
    m = re.search(r"\{.*\}", content, flags=re.S)
    if m:
        return json.loads(m.group(0))
    raise ValueError(f"JSON parse failed. head={content[:120]!r}")

def load_recent_raw_groups(db, window_sec=6 * 60 * 60, prefix_bits=16):
    now = int(time.time())
    since = now - window_sec
    q = db.collection("raw_articles").where(filter=FieldFilter("published_at", ">=", since))
    groups = defaultdict(list)
    for d in q.stream():
        it = d.to_dict() or {}
        k = sim_prefix(it.get("simhash", ""), prefix_bits=prefix_bits)
        groups[k].append((d.id, it))
    print(f"Loaded {len(groups)} clusters from raw_articles")
    return groups

def already_generated(db, cluster_key):
    snap = db.collection("generated_articles").where(filter=FieldFilter("cluster_key", "==", cluster_key)).limit(1).get()
    return len(snap) > 0

def make_payload_from_sources(items):
    n = len(items)
    title = f"[Auto] {n} source{'s' if n > 1 else ''} on same event"
    summary = (
        "Multiple outlets reported a similar event. (Template summary: LLM disabled)"
        if n > 1
        else "A single source reported this event. (Template summary: LLM disabled)"
    )
    bullets = ["Key point 1", "Key point 2", "Key point 3"]
    first = items[0][1] if items else {}
    facts = [{"text": first.get("title", ""), "evidence_url": first.get("url", "")}]
    actions = {
        "stock": [{
            "action": "Watch related tickers",
            "assumptions": "News momentum possible",
            "risk": "Rumor/overreaction",
            "alternative": "Stage entries"
        }],
        "futures": [{
            "action": "Small sector ETF probe",
            "assumptions": "Sector beta to news",
            "risk": "Macro shocks",
            "alternative": "Options spread"
        }],
        "biz": [{
            "action": "Monitor supplier/customer notes",
            "assumptions": "Lead-time/price impact",
            "risk": "Overreacting pre-confirmation",
            "alternative": "Phase-in after cross-check"
        }],
    }
    return {"title": title, "summary": summary, "bullets": bullets, "facts": facts, "actions": actions}

def run_once():
    db = init_db()
    groups = load_recent_raw_groups(db)
    created = 0

    for cluster_key, items in groups.items():
        if len(items) < 1:
            continue
        if already_generated(db, cluster_key):
            print(f"Skipping cluster {cluster_key}: already generated")
            continue

        src_lines = []
        ts_min, ts_max = 10 ** 12, 0
        for _id, it in items:
            src_lines.append(f"- {it.get('title', '')} | {it.get('url', '')}")
            ts = int(it.get("published_at", 0) or 0)
            ts_min, ts_max = min(ts_min, ts), max(ts_max, ts)

        payload = make_payload_from_sources(items)
        token_usage = {"prompt": 0, "completion": 0}
        latency_ms = 0
        model_used = "template"

        if USE_OPENAI and len(src_lines) >= 1:
            try:
                t0 = time.time()
                prompt = PROMPT.format(sources="\n".join(src_lines))
                print(f"Sending OpenAI request for cluster {cluster_key} with {len(src_lines)} sources")
                resp = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.2,
                    response_format=ResponseFormat.JSON_OBJECT,
                )
                latency_ms = int((time.time() - t0) * 1000)

                try:
                    token_usage["prompt"] = getattr(resp.usage, "prompt_tokens", 0)
                    token_usage["completion"] = getattr(resp.usage, "completion_tokens", 0)
                except Exception:
                    pass

                content = getattr(resp.choices[0].message, "content", None)
                if content is None and isinstance(resp.choices[0].message, dict):
                    content = resp.choices[0].message.get("content", "")

                # ✅ 디버깅 출력
                print("🔎 LLM RESPONSE START")
                print(content)
                print("🔎 LLM RESPONSE END")

                payload = safe_parse_json(content)
                model_used = "gpt-4o-mini"

            except Exception as e:
                print(f"OpenAI error for cluster {cluster_key}: {repr(e)}")
                print("Trace:\n", traceback.format_exc())
                log_event(db, "openai_error", {
                    "msg": str(e),
                    "raw_content": content if 'content' in locals() else "N/A",
                    "cluster_key": cluster_key
                })

        doc = {
            "cluster_key": cluster_key,
            "title": payload.get("title", ""),
            "summary": payload.get("summary", ""),
            "bullets": payload.get("bullets", []),
            "facts": payload.get("facts", []),
            "actions": payload.get("actions", {"stock": [], "futures": [], "biz": []}),
            "evidence_urls": [line.split("|")[-1].strip() for line in src_lines if "|" in line],
            "raw_refs": [x[0] for x in items],
            "published_window": {"start": ts_min, "end": ts_max},
            "model": model_used,
            "token_usage": token_usage,
            "latency_ms": latency_ms,
            "created_at": firestore.SERVER_TIMESTAMP,
        }
        db.collection("generated_articles").add(doc)
        created += 1
        print(f"Generated article for cluster {cluster_key}, total created={created}")

    log_event(db, "generate_done", {"created": created})
    print(f"Found {len(groups)} clusters, generated={created}")

if __name__ == "__main__":
    run_once()
