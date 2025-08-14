# 역할: raw_articles를 simhash prefix로 군집 → 2개 이상 모인 군집만 LLM(옵션)으로 재구성 → generated_articles 저장
# (원문 전문 저장 안 함. 증거 링크는 수집한 기사 URL 사용)


import os, json, time
from collections import defaultdict
from common import init_db, log_event, sim_prefix
from firebase_admin import firestore

USE_OPENAI = bool(os.getenv("OPENAI_API_KEY"))
if USE_OPENAI:
    from openai import OpenAI
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# LLM 프롬프트 (짧고 JSON 강제)
PROMPT = """You are a news rewrite assistant.
Given multiple sources about the same event, produce STRICT JSON:
{
 "title": str,
 "summary": str,           // 600 chars max, factual, cautious
 "bullets": [str, str, str],
 "facts": [{"text": str, "evidence_url": str}],
 "actions": {
   "stock":[{"action":"","assumptions":"","risk":"","alternative":""}],
   "futures":[{"action":"","assumptions":"","risk":"","alternative":""}],
   "biz":[{"action":"","assumptions":"","risk":"","alternative":""}]
 }
}
Rules:
- Cite at least 2 evidence_url from given list.
- No advice; cautious tone; avoid guarantees.
- If mostly Korean sources, write Korean; otherwise English.
Sources:
{sources}
"""

def load_recent_raw_groups(db, window_sec=6*60*60, prefix_bits=16):
    now = int(time.time())
    since = now - window_sec
    q = db.collection("raw_articles").where("published_at", ">=", since)
    groups = defaultdict(list)
    for d in q.stream():
        it = d.to_dict() or {}
        k = sim_prefix(it.get("simhash",""), prefix_bits=prefix_bits)
        groups[k].append((d.id, it))
    return groups

def already_generated(db, cluster_key):
    snap = db.collection("generated_articles").where("cluster_key","==",cluster_key).limit(1).get()
    return len(snap) > 0

def make_payload_from_sources(items):
    # LLM 미사용 시 템플릿
    title = f"[Auto] {len(items)} sources on same event"
    summary = "Multiple outlets reported a similar event. (Template summary: LLM disabled)"
    bullets = ["Key point 1", "Key point 2", "Key point 3"]
    facts = [{"text": items[0][1].get("title",""), "evidence_url": items[0][1].get("url","")}]
    actions = {
        "stock":[{"action":"Watch related tickers","assumptions":"News momentum possible","risk":"Rumor/overreaction","alternative":"Stage entries"}],
        "futures":[{"action":"Small sector ETF probe","assumptions":"Sector beta to news","risk":"Macro shocks","alternative":"Options spread"}],
        "biz":[{"action":"Monitor supplier/customer notes","assumptions":"Lead-time/price impact","risk":"Overreacting pre-confirmation","alternative":"Phase-in after cross-check"}],
    }
    return {"title": title, "summary": summary, "bullets": bullets, "facts": facts, "actions": actions}

def run_once():
    db = init_db()
    groups = load_recent_raw_groups(db)
    created = 0

    for cluster_key, items in groups.items():
        # 유사 기사만 묶는다: 최소 2개 이상
        if len(items) < 2:
            continue
        if already_generated(db, cluster_key):
            continue

        # 소스 문자열 (제목 | URL) 나열
        src_lines = []
        ts_min, ts_max = 10**12, 0
        for _id, it in items:
            src_lines.append(f"- {it.get('title','')} | {it.get('url','')}")
            ts = int(it.get("published_at", 0))
            ts_min, ts_max = min(ts_min, ts), max(ts_max, ts)

        payload = make_payload_from_sources(items)
        token_usage = {"prompt":0, "completion":0}
        latency_ms = 0

        if USE_OPENAI:
            try:
                t0 = time.time()
                prompt = PROMPT.format(sources="\n".join(src_lines))
                resp = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role":"user","content":prompt}],
                    temperature=0.2,
                    response_format={"type":"json_object"},
                )
                latency_ms = int((time.time()-t0)*1000)
                token_usage["prompt"] = getattr(resp.usage, "prompt_tokens", 0)
                token_usage["completion"] = getattr(resp.usage, "completion_tokens", 0)
                payload = json.loads(resp.choices[0].message.content)
            except Exception as e:
                log_event(db, "openai_error", {"msg": str(e)})
                # 템플릿으로 폴백

        doc = {
            "cluster_key": cluster_key,
            "title": payload.get("title",""),
            "summary": payload.get("summary",""),
            "bullets": payload.get("bullets",[]),
            "facts": payload.get("facts",[]),
            "actions": payload.get("actions",{"stock":[],"futures":[],"biz":[]}),
            "evidence_urls": [line.split("|")[-1].strip() for line in src_lines if "|" in line],
            "raw_refs": [x[0] for x in items],
            "published_window": {"start": ts_min, "end": ts_max},
            "model": "gpt-4o-mini" if USE_OPENAI else "template",
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
