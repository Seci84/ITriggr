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
# UI 구조에 맞춰 insights/actions 스키마 확장 (general, entrepreneur, politician, investor)

PROMPT = """You are a news rewrite assistant. Return ONLY a single JSON object with no code fences, no explanations, and no comments.

Required JSON shape (all fields are MANDATORY and must match exactly):
{{
  "title": "string",
  "summary": "string",
  "bullets": ["string", "string", "string"],
  "facts": [{{"text":"string","evidence_url":"string"}}],
  "insights": {{
    "general": "string",
    "entrepreneur": "string",
    "politician": "string",
    "investor": "string"
  }},
  "actions": {{
    "general": [{{"action":"string","assumptions":"string","risk":"string","alternative":"string"}}],
    "entrepreneur": [{{"action":"string","assumptions":"string","risk":"string","alternative":"string"}}],
    "politician": [{{"action":"string","assumptions":"string","risk":"string","alternative":"string"}}],
    "investor": [{{"action":"string","assumptions":"string","risk":"string","alternative":"string"}}]
  }}
}}

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

from urllib.parse import urlparse

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

    # ✅ UI가 요구하는 reader_types에 맞춰 기본 insights/actions 채우기
    insights = {
        "general": "이 이슈는 일상 생활/직장 환경에 점진적 영향을 미칠 수 있습니다.",
        "entrepreneur": "시장 공백과 규제 변화에서 초기 진입 기회를 탐색하십시오.",
        "politician": "법/규제 업데이트 필요성과 국제 공조 이슈를 점검하십시오.",
        "investor": "테마/섹터 리스크와 펀더멘털 괴리를 구분해 모니터링하십시오."
    }
    actions = {
        "general": [{
            "action": "공식 보도자료 및 정부 발표를 팔로업해 핵심 변경사항 파악",
            "assumptions": "정책/기업 발표가 실제 행동으로 이어질 가능성",
            "risk": "초기 보도 과장 또는 정정 보도",
            "alternative": "전문가 뉴스레터 구독으로 2차 검증"
        }],
        "entrepreneur": [{
            "action": "고객 페인포인트 인터뷰 및 린 프로토타입 테스트",
            "assumptions": "이슈가 실수요 문제를 유발/확대",
            "risk": "시장 수요 과대평가",
            "alternative": "PoC/파일럿으로 리스크 분산"
        }],
        "politician": [{
            "action": "이해관계자 간담회 개최 및 영향평가(규제/고용/안전) 착수",
            "assumptions": "정책 개입 여지 존재",
            "risk": "정치적 반발/예산 제약",
            "alternative": "권고안/가이드라인부터 단계적 추진"
        }],
        "investor": [{
            "action": "섹터 ETF로 테마 익스포저 소규모 실험",
            "assumptions": "뉴스 모멘텀이 단기 가격 변동 유발",
            "risk": "거시 변수에 의한 역풍",
            "alternative": "현금비중/헷지로 변동성 관리"
        }]
    }

    return {
        "title": title,
        "summary": summary,
        "bullets": bullets,
        "facts": facts,
        "insights": insights,
        "actions": actions
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

        # Firestore 문서 구성 (UI가 읽는 필드 포함)
        doc = {
            "cluster_key": cluster_key,
            "title": payload.get("title", ""),
            "summary": payload.get("summary", ""),
            "bullets": payload.get("bullets", []),
            "facts": payload.get("facts", []),
            # ✅ UI에서 사용하는 필드 추가
            "insights": payload.get("insights", {
                "general": "", "entrepreneur": "", "politician": "", "investor": ""
            }),
            "actions": payload.get("actions", {
                "general": [], "entrepreneur": [], "politician": [], "investor": []
            }),
            "evidence_urls": [line.split("|")[1].strip() for line in src_lines if "|" in line],
            "raw_refs": [x[0] for x in items],
            "published_window": {"start": ts_min, "end": ts_max},
            "model": model_used,
            "token_usage": token_usage,
            "latency_ms": latency_ms,
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
