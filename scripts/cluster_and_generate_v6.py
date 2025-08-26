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
import requests
from bs4 import BeautifulSoup

# --- OpenAI 사용 여부 ---
client = None
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
USE_OPENAI = os.getenv("USE_OPENAI", "False").lower() == "true"

if OPENAI_API_KEY and USE_OPENAI:
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        print("✅ OpenAI client initialized successfully")
    except Exception as e:
        print(f"❌ OpenAI client init failed: {e}")
        traceback.print_exc()
        USE_OPENAI = False
else:
    print(f"USE_OPENAI = {USE_OPENAI}, OPENAI_API_KEY is {'set' if OPENAI_API_KEY else 'not set'}")

# --- LLM 프롬프트: JSON만! (주석/코드펜스 금지) ---
PROMPT = """You are a news rewrite assistant. Return ONLY a single JSON object with no code fences, no explanations, and no comments.

Required JSON shape (all fields are MANDATORY and must match exactly):
{
  "title": "string",
  "summary": "string",
  "bullets": ["string", "string", "string"],
  "facts": [{"text":"string","evidence_url":"string"}],
  "insights": {
    "general": "string",
    "entrepreneur": "string",
    "politician": "string",
    "investor": "string"
  },
  "actions": {
    "general": [{"action":"string","assumptions":"string","risk":"string","alternative":"string"}],
    "entrepreneur": [{"action":"string","assumptions":"string","risk":"string","alternative":"string"}],
    "politician": [{"action":"string","assumptions":"string","risk":"string","alternative":"string"}],
    "investor": [{"action":"string","assumptions":"string","risk":"string","alternative":"string"}]
  }
}

Rules:
- Strictly adhere to the exact JSON shape above. Any deviation (e.g., comments, code blocks, explanations) will result in rejection.
- Detect the category (politics, economy, society, tech, military, etc.) from the content and tailor the analysis to it (e.g., tech: focus on innovations, military: strategic implications).
- Use available sources (one or more). Cite at least 1 item in "facts" with evidence_url chosen from the given Sources list. Be specific: name companies, products, or laws.
- Analyze the full content of each source URL to inform the title, summary, bullets, facts, insights, and actions. Provide multi-faceted information: e.g., market size, specific examples, related entities.
- Use a cautious, factual tone. No guarantees/advice. Use phrases like "possible idea" or "consider exploring".
- If mostly Korean sources, write Korean; otherwise English.
- For insights and actions, generate specific, concrete suggestions based on reader type:
  - General: Suggest skill learning for career opportunities (e.g., "Learn quantum computing via Coursera for roles like Astronautical Engineer at SpaceX") and small investments (e.g., "Specific US ETF: ARKX or UFO with SpaceX exposure").
  - Entrepreneur: Propose business opportunities like M&A or partnerships, naming specific companies (e.g., "Quantum Technologies for laser comm patents") and challenges (e.g., "Boeing's supply chain issues").
  - Politician: Recommend legislation or diplomacy (e.g., "Strengthen Space Policy Directives for quantum navigation; address gaps in international accords like Artemis").
  - Investor: Advise on stocks, chained opportunities, and troubled firms (e.g., "Invest in ARKX ETF for SpaceX exposure; ULA facing market share loss to SpaceX").

Sources:
{sources}
"""

def safe_parse_json(content: str):
    """LLM 응답에서 JSON만 안전하게 추출."""
    try:
        # 코드블록 및 설명 제거
        content = re.sub(r"^```(?:json)?\s*|\s*```$|^.*?: |^Explanation: .*", "", content.strip(), flags=re.MULTILINE)
        # JSON 객체 처리 시도
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            # 객체 추출 시도
            match = re.search(r"\{(?:[^{}]|(?R))*\}", content, flags=re.DOTALL)
            if match:
                return json.loads(match.group(0))
    except Exception as e:
        print(f"Debug: Failed to parse JSON - Error: {e}, Content: {content[:500]}")
    raise ValueError(f"JSON parse failed. head={content[:120]!r}")

def fetch_content(url, items=None):
    """URL에서 기사 본문 추출."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        paragraphs = soup.find_all('p')
        content = ' '.join(p.get_text() for p in paragraphs if p.get_text().strip())
        return content[:1000]
    except Exception as e:
        print(f"Failed to fetch content from {url}: {e}")
        return items[0][1].get("title", "No content available") if items else "No content available"

def load_recent_raw_groups(db, window_sec=6 * 60 * 60, prefix_bits=16):
    now = int(time.time())
    since = now - window_sec
    q = db.collection("raw_articles").where(filter=FieldFilter("published_at", ">=", since))
    groups = defaultdict(list)
    for d in q.stream():
        it = d.to_dict() or {}
        if "nytimes.com" not in it.get("url", ""):  # NYT 제외 활성화
            k = sim_prefix(it.get("simhash", ""), prefix_bits=prefix_bits)
            groups[k].append((d.id, it))
    print(f"Loaded {len(groups)} clusters from raw_articles (NYT excluded)")
    return groups

def already_generated(db, cluster_key):
    snap = db.collection("generated_articles_v3").where(filter=FieldFilter("cluster_key", "==", cluster_key)).limit(1).get()
    return len(snap) > 0

def make_payload_from_sources(items):
    n = len(items)
    title = f"[Auto] {n} source{'s' if n > 1 else ''} on same event"
    summary = (
        "Multiple outlets reported a similar event. (Template summary)"
        if n > 1
        else "A single source reported this event. (Template summary)"
    )
    first = items[0][1] if items else {}
    bullets = [f"Event reported by {first.get('source', 'unknown')}", "Details unavailable", f"Source: {first.get('url', 'unknown')}"]
    facts = [{"text": first.get("title", "No title"), "evidence_url": first.get("url", "")}]
    actions = {
        "general": [{"action": "Explore online tech courses", "assumptions": "Job growth possible", "risk": "Uncertain", "alternative": "Monitor news"}],
        "entrepreneur": [{"action": "Check tech firm partnerships", "assumptions": "Innovation potential", "risk": "Uncertainty", "alternative": "Research"}],
        "politician": [{"action": "Review tech policies", "assumptions": "Legislative needs", "risk": "Delay", "alternative": "Consult"}],
        "investor": [{"action": "Monitor tech ETFs", "assumptions": "Market movement", "risk": "Volatility", "alternative": "Diversify"}]
    }
    insights = {
        "general": "Potential tech career opportunities",
        "entrepreneur": "Possible tech partnership opportunities",
        "politician": "Tech policy consideration needed",
        "investor": "Tech sector movement possible"
    }
    return {"title": title, "summary": summary, "bullets": bullets, "facts": facts, "insights": insights, "actions": actions}

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
        if not items:
            src_lines.append("- No data available | N/A | No content")
        else:
            for _id, it in items:
                url = it.get("url", "").replace("{", "{{").replace("}", "}}")  # URL 이스케이프
                title = it.get("title", "No title available").replace("{", "{{").replace("}", "}}")  # Title 이스케이프
                content = fetch_content(url, items).replace("{", "{{").replace("}", "}}")  # Content 이스케이프
                src_lines.append(f"- {title} | {url} | {content}")
        ts_min, ts_max = 10 ** 12, 0
        for _id, it in items:
            ts = int(it.get("published_at", 0) or 0)
            ts_min, ts_max = min(ts_min, ts), max(ts_max, ts)

        payload = None
        token_usage = {"prompt": 0, "completion": 0}
        latency_ms = 0
        model_used = "template"

        if USE_OPENAI and len(src_lines) >= 1:
            try:
                print(f"Debug: PROMPT template: {PROMPT[:500]}")
                print(f"Debug: src_lines for cluster {cluster_key}: {src_lines}")
                prompt = PROMPT.format(sources="\n".join(src_lines))
                print(f"Debug: Generated prompt for cluster {cluster_key}: {prompt[:1000]}")
                print(f"Sending OpenAI request for cluster {cluster_key}...")
                t0 = time.time()
                resp = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.2,
                    response_format={"type": "json_object"},
                )
                latency_ms = int((time.time() - t0) * 1000)
                print(f"Debug: Raw response for cluster {cluster_key}: {resp.choices[0].message.content[:1000]}")

                try:
                    token_usage["prompt"] = getattr(resp.usage, "prompt_tokens", 0)
                    token_usage["completion"] = getattr(resp.usage, "completion_tokens", 0)
                except Exception:
                    pass

                content = getattr(resp.choices[0].message, "content", None)
                if content is None:
                    print(f"⚠️ No content returned from OpenAI for cluster {cluster_key}")
                    payload = make_payload_from_sources(items)
                else:
                    print("🔎 LLM RESPONSE START")
                    print(content[:1000])
                    print("🔎 LLM RESPONSE END")
                    payload = safe_parse_json(content)
                    # 구조 검증
                    if not all(k in payload for k in ["insights", "actions"]) or \
                       not all(k in payload["insights"] for k in ["general", "entrepreneur", "politician", "investor"]) or \
                       not all(k in payload["actions"] for k in ["general", "entrepreneur", "politician", "investor"]):
                        print(f"⚠️ Invalid structure for cluster {cluster_key}, using template")
                        payload = make_payload_from_sources(items)
                    model_used = "gpt-4o-mini"

            except Exception as e:
                print(f"OpenAI error for cluster {cluster_key}: {repr(e)}")
                traceback.print_exc()
                log_event(db, "openai_error", {
                    "msg": str(e),
                    "raw_content": content if 'content' in locals() else "N/A",
                    "cluster_key": cluster_key
                })
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
            "insights": payload.get("insights", {"general": "", "entrepreneur": "", "politician": "", "investor": ""}),
            "actions": payload.get("actions", {"general": [], "entrepreneur": [], "politician": [], "investor": []}),
            "evidence_urls": [line.split("|")[1].strip() for line in src_lines if "|" in line],
            "raw_refs": [x[0] for x in items],
            "published_window": {"start": ts_min, "end": ts_max},
            "model": model_used,
            "token_usage": token_usage,
            "latency_ms": latency_ms,
            "created_at": firestore.SERVER_TIMESTAMP,
        }
        db.collection("generated_articles_v3").add(doc)
        created += 1
        print(f"Generated article for cluster {cluster_key}, total created={created}")

    log_event(db, "generate_done", {"created": created})
    print(f"Found {len(groups)} clusters, generated={created}")

if __name__ == "__main__":
    run_once()
