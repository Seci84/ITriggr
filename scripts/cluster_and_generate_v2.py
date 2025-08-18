# scripts/cluster_and_generate.py
# 역할: raw_articles를 simhash prefix로 군집 → (1개 이상도 OK) LLM(옵션) 재구성 → generated_articles 저장
# (원문 전문 저장 안 함. 증거 링크는 수집한 기사 URL 사용)

import os
import re
import json
import time
import traceback
from collections import defaultdict
from firebase_admin import firestore
from common import init_db, log_event, sim_prefix

# --- OpenAI 사용 여부 ---
USE_OPENAI = bool(os.getenv("OPENAI_API_KEY"))
if USE_OPENAI:
    from openai import OpenAI
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
print("USE_OPENAI =", USE_OPENAI)

# --- LLM 프롬프트: JSON만! (주석/코드펜스 금지) ---
PROMPT = """You are a news rewrite assistant.
Return ONLY a single JSON object. No code fences, no explanations, no comments.

Required JSON shape:
{
  "title": "string",
  "summary": "string",
  "bullets": ["string", "string", "string"],
  "facts": [{"text":"string","evidence_url":"string"}],
  "actions": {
    "stock":[{"action":"","assumptions":"","risk":"","alternative":""}],
    "futures":[{"action":"","assumptions":"","risk":"","alternative":""}],
    "biz":[{"action":"","assumptions":"","risk":"","alternative":""}]
  }
}

Rules:
- Use available sources (one or more). Cite at least 1 item in "facts" with evidence_url chosen from the given Sources list.
- Cautious, factual tone. No guarantees/advice.
- If mostly Korean sources, write Korean; otherwise English.

Sources:
{sources}
"""

def safe_parse_json(content: str):
    """LLM 응답에서 JSON만 안전하게 추출."""
    # 1) 그대로 파싱
    try:
        return json.loads(content)
    except Exception:
        pass
    # 2) 코드펜스 제거 후 파싱
    content2 = re.sub(r"^```(?:json)?\s*|\s*```$", "", content.strip(), flags=re.I | re.M)
    try:
        return json.loads(content2)
    except Exception:
        pass
    # 3) 가장 바깥 중괄호 블록만 추출해서 파싱
    m = re.search(r"\{.*\}", content, flags=re.S)
    if m:
        return json.loads(m.group(0))
    raise ValueError(f"JSON parse failed. head={content[:120]!r}")

def load_recent_raw_groups(db, window_sec=6 * 60 * 60, prefix_bits=16):
    now = int(time.time())
    since = now - window_sec
    q = db.collection("raw_articles").where("published_at", ">=", since)
    groups = defaultdict(list)
    for d in q.stream():
        it = d.to_dict() or {}
        k = sim_prefix(it.get("simhash", ""), prefix_bits=prefix_bits)
        groups[k].append((d.id, it))
    return groups

def already_generated(db, cluster_key):
    snap = db.collection("generated_articles").where("cluster_key", "==", cluster_key).limit(1).get()
    return len(snap) > 0

def make_payload_from_sources(items):
    """LLM 미사용/실패 시 템플릿 페이로드."""
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
        # ✅ 1개짜리 군집도 생성
        if len(items) < 1:
            continue
        if already_generated(db, cluster_key):
            continue

        # 소스 문자열 (제목 | URL) 나열
        src_lines = []
        ts_min, ts_max = 10 ** 12, 0
        for _id, it in items:
            src_lines.append(f"- {it.get('title', '')} | {it.get('url', '')}")
            ts = int(it.get("published_at", 0) or 0)
            ts_min, ts_max = min(ts_min, ts), max(ts_max, ts)

        payload = make_payload_from_sources(items)  # 템플릿 기본값
        token_usage = {"prompt": 0, "completion": 0}
        latency_ms = 0
        model_used = "template"

        # ✅ 소스가 1개라도 있으면 LLM 호출 시도
        if USE_OPENAI and len(src_lines) >= 1:
            try:
                t0 = time.time()
                prompt = PROMPT.format(sources="\n".join(src_lines))
                resp = client.chat.completions.create(
                    model="gpt-4o",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.2,
                    response_format={"type": "json_object"},
                )
                latency_ms = int((time.time() - t0) * 1000)

                # usage (SDK 버전 안전 접근)
                try:
                    token_usage["prompt"] = getattr(resp.usage, "prompt_tokens", 0)
                    token_usage["completion"] = getattr(resp.usage, "completion_tokens", 0)
                except Exception:
                    pass

                # 메시지 파싱 (속성/딕셔너리 모두 대응)
                content = getattr(resp.choices[0].message, "content", None)
                if content is None and isinstance(resp.choices[0].message, dict):
                    content = resp.choices[0].message.get("content", "")
                payload = safe_parse_json(content)
                model_used = "gpt-4o"
            except Exception as e:
                # 콘솔 + Firestore에 모두 기록 후 템플릿 유지
                print("OpenAI error:", repr(e))
                print("Trace:\n", traceback.format_exc())
                log_event(db, "openai_error", {"msg": str(e)})

        # 저장 문서 구성
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
            "model": model_used,                # ← 실제 사용 모델만 기록
            "token_usage": token_usage,
            "latency_ms": latency_ms,
            "created_at": firestore.SERVER_TIMESTAMP,
        }
        db.collection("generated_articles").add(doc)
        created += 1

    log_event(db, "generate_done", {"created": created})
    print(f"generated={created}")

if __name__ == "__main__":
    run_once()
