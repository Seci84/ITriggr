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
from urllib.parse import urlparse

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

# --- LLM 프롬프트 (JSON only) ---
# ✅ 스키마를 talks(구어체 문단) 중심으로 변경
PROMPT = """You are a news rewrite assistant. Return ONLY a single JSON object with no code fences, no explanations, and no comments.

Required JSON shape (all fields are MANDATORY and must match exactly):
{
  "title": "string",
  "summary": "string",
  "bullets": ["string", "string", "string"],
  "facts": [{"text":"string","evidence_url":"string"}],
  "talks": {
    "general": "string",
    "entrepreneur": "string",
    "politician": "string",
    "investor": "string"
  }
}

Rules:
- Strictly adhere to the exact JSON shape above. Any deviation (e.g., comments, code blocks, explanations) will result in rejection.
- Detect the category (politics, economy, society, tech, military, etc.) from the content and tailor the analysis.
- Use available sources (one or more). Cite at least 1 item in "facts" with evidence_url chosen from the given Sources list. Be specific with entities (companies, products, laws).
- Analyze the full content of each source URL to inform title, summary, bullets, facts, and talks.
- Tone: cautious and factual. No guarantees or advice. Use phrases like "consider", "possible idea".
- Language: If most sources are Korean, write Korean; otherwise English.
- "talks" must be a conversational paragraph (2–4 sentences each, in the selected language) that naturally weaves together an action suggestion, the underlying assumption/context, a risk to watch, and a practical alternative. Avoid bullet-like structure; write as smooth natural language.
- Avoid financial or policy advice; keep it interpretive and neutral.

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

def fetch_content(url):
    """URL에서 기사 본문 추출."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        # 간단한 본문 추출 (사이트별로 조정 필요)
        paragraphs = soup.find_all('p')
        content = ' '.join(p.get_text() for p in paragraphs if p.get_text().strip())
        return content[:1000]  # 토큰 제한으로 1000자 제한
    except Exception as e:
        print(f"Failed to fetch content from {url}: {e}")
        return "Content unavailable"

def load_recent_raw_groups(db, window_sec=6 * 60 * 60, prefix_bits=16,
                           exclude_domains=("nytimes.com", "nyti.ms")):
    """
    최근 원문 기사들을 simhash prefix로 클러스터링.
    NYT 도메인(nytimes.com, nyti.ms)은 제외.
    """
    def _is_excluded(url: str) -> bool:
        try:
            host = urlparse(url or "").netloc.lower()
            if not host:
                return False
            # example: sub.domain.nytimes.com 도 함께 제외
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
        total += 1
        url = it.get("url", "") or ""
        if _is_excluded(url):
            skipped += 1
            continue

        k = sim_prefix(it.get("simhash", ""), prefix_bits=prefix_bits)
        groups[k].append((d.id, it))

    print(f"Loaded {len(groups)} clusters from raw_articles (total_docs={total}, skipped_nyt={skipped})")
    return groups

def already_generated(db, cluster_key: str) -> bool:
    """
    특정 cluster_key에 대해 이미 생성된 문서가 있는지 확인.
    (generated_articles_v3 기준)
    """
    try:
        snap = (db.collection("generated_articles_v3")
                  .where(filter=FieldFilter("cluster_key", "==", cluster_key))
                  .limit(1)
                  .get())
        return len(snap) > 0
    except Exception as e:
        print(f"already_generated check failed: {e}")
        return False

# ✅ 템플릿(LLM 비활성/실패 시)도 talks만 생성하도록 수정
def make_payload_from_sources(items):
    """OPENAI 비활성/실패 시 UI가 바로 쓸 수 있는 템플릿 페이로드."""
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

    talks = {
        "general": "이번 이슈는 일상과도 맞닿아 있어요. 단정하기보다는 주변 의견을 들어보며 상황을 천천히 정리해 보세요. 과열된 주장엔 거리를 두고, 도움이 되는 작은 실천부터 시작하면 좋아요.",
        "entrepreneur": "시장 반응이 출렁일 수 있으니 과감한 메시지보다 고객 인터뷰와 작은 실험으로 검증해요. 리스크 노출은 최소화하고, 대안 채널이나 파일럿으로 학습 속도를 높여보죠.",
        "politician": "사실관계를 우선 확인하고 이해관계자 의견을 폭넓게 수렴해 보세요. 정쟁으로 번질 여지가 있다면 단계적 권고안부터 제시하는 편이 안전합니다.",
        "investor": "헤드라인보다 펀더멘털과 현금흐름을 먼저 살펴봐요. 변동성은 분산과 포지션 크기 조절로 관리하고, 정보가 더 쌓일 때까지는 관망도 선택지예요."
    }

    return {
        "title": title,
        "summary": summary,
        "bullets": bullets,
        "facts": facts,
        "talks": talks
    }

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
            url = it.get("url", "")
            title = it.get("title", "")
            content = fetch_content(url)  # URL에서 본문 가져오기
            src_lines.append(f"- {title} | {url} | {content}")
            ts = int(it.get("published_at", 0) or 0)
            ts_min, ts_max = min(ts_min, ts), max(ts_max, ts)

        payload = None  # 초기화
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
                    response_format={"type": "json_object"},
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
                # GPT 실패 시 템플릿 사용
                payload = make_payload_from_sources(items)

        else:
            # OpenAI 비활성화 시 템플릿 사용
            payload = make_payload_from_sources(items)

        if payload is None:
            payload = make_payload_from_sources(items)  # 안전망

        # Firestore 문서 구성 (talks 중심)
        doc = {
            "cluster_key": cluster_key,
            "title": payload.get("title", ""),
            "summary": payload.get("summary", ""),
            "bullets": payload.get("bullets", []),
            "facts": payload.get("facts", []),
            # ✅ 새 구조: 구어체 문단
            "talks": payload.get("talks", {
                "general": "", "entrepreneur": "", "politician": "", "investor": ""
            }),
            # 참고/추적용 메타
            "evidence_urls": [line.split("|")[1].strip() for line in src_lines if "|" in line],
            "raw_refs": [x[0] for x in items],
            "published_window": {"start": ts_min, "end": ts_max},
            "model": model_used,
            "token_usage": token_usage,
            "latency_ms": latency_ms,
            "schema_version": "talks_v1",   # ✅ 스키마 식별용
            "created_at": firestore.SERVER_TIMESTAMP,
        }

        # ✅ 컬렉션 이름을 v3로 저장 (UI와 일치)
        db.collection("generated_articles_v3").add(doc)
        created += 1
        print(f"Generated article for cluster {cluster_key}, total created={created}")

    log_event(db, "generate_done", {"created": created})
    print(f"Found {len(groups)} clusters, generated={created}")

if __name__ == "__main__":
    run_once()
