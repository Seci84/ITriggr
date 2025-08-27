import streamlit as st
import requests
import json
import firebase_admin
from firebase_admin import credentials, auth, firestore
from datetime import datetime, UTC
from typing import List, Dict
from openai import OpenAI

st.set_page_config(page_title="ITRiggr - News", page_icon="📰", layout="wide")

# ========================
# 글로벌 스타일 (여백 + 카드 + 타이포)
# ========================
st.markdown("""
<style>
/* 전체 컨테이너 폭과 좌우 여백(독자 시선 중앙 집중) */
.block-container {
  max-width: 1200px;
  padding-left: 2.5rem;
  padding-right: 2.5rem;
}

/* 카드 공통 */
.card {
  border: 1px solid #eaeaea;
  border-radius: 14px;
  padding: 16px 18px;
  background: #ffffff;
  box-shadow: 0 2px 8px rgba(0,0,0,0.04);
}

/* 저널 느낌 타이포 */
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@700&family=Merriweather:wght@400;700&display=swap');

.article-title {
  font-family: 'Playfair Display', serif;
  font-size: 1.8rem;
  line-height: 1.25;
  margin: 0.2rem 0 0.4rem 0;
}

.hero-title { font-size: 2.2rem; }
.side-title { font-size: 1.6rem; }

.article-meta {
  color: rgba(0,0,0,0.6);
  font-size: 0.9rem;
  margin-bottom: 0.6rem;
}

.article-summary {
  font-family: 'Merriweather', serif;
  font-size: 1.05rem;
  line-height: 1.65;
  margin-bottom: 0.6rem;
}

.article-section-title {
  font-weight: 700;
  margin-top: 0.8rem;
  margin-bottom: 0.2rem;
}

/* 카드 마커를 가진 컨테이너를 카드처럼 보이게 */
div[data-testid="stVerticalBlock"]:has(> .itr-card-marker) {
  border: 1px solid #eaeaea;
  border-radius: 14px;
  padding: 16px 18px;
  background: #ffffff;
  box-shadow: 0 2px 8px rgba(0,0,0,0.04);
}

/* 마커 자체는 보이지 않게 */
.itr-card-marker { display: none; }

/* ── Column separators: 마커 다음에 오는 가로 컬럼 블록에만 적용 ── */
.itr-row-start + div[data-testid="stHorizontalBlock"] > div[data-testid="column"] {
  padding-left: 12px;
  padding-right: 12px;
}
.itr-row-start + div[data-testid="stHorizontalBlock"] > div[data-testid="column"]:not(:last-child) {
  border-right: 1px solid rgba(0,0,0,0.08); /* 얇은 선 */
}

/* 모바일에선 세퍼레이터 감춤 (원하면 제거 가능) */
@media (max-width: 900px) {
  .itr-row-start + div[data-testid="stHorizontalBlock"] > div[data-testid="column"]:not(:last-child) {
    border-right: none;
  }
}

</style>
""", unsafe_allow_html=True)

# ========================
# Firebase Admin 초기화
# ========================
if not firebase_admin._apps:
    cred = credentials.Certificate({
        "type": st.secrets["FIREBASE_TYPE"],
        "project_id": st.secrets["FIREBASE_PROJECT_ID"],
        "private_key_id": st.secrets["FIREBASE_PRIVATE_KEY_ID"],
        "private_key": st.secrets["FIREBASE_PRIVATE_KEY"].replace("\\n", "\n"),
        "client_email": st.secrets["FIREBASE_CLIENT_EMAIL"],
        "client_id": st.secrets["FIREBASE_CLIENT_ID"],
        "auth_uri": st.secrets["FIREBASE_AUTH_URI"],
        "token_uri": st.secrets["FIREBASE_TOKEN_URI"],
        "auth_provider_x509_cert_url": st.secrets["FIREBASE_AUTH_PROVIDER_X509_CERT_URL"],
        "client_x509_cert_url": st.secrets["FIREBASE_CLIENT_X509_CERT_URL"],
    })
    firebase_admin.initialize_app(cred)

db = firestore.client()
WEB_API_KEY = st.secrets.get("FIREBASE_API_KEY")
OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY")

# ========================
# Auth REST endpoints
# ========================
SIGN_UP_URL = f"https://identitytoolkit.googleapis.com/v1/accounts:signUp?key={WEB_API_KEY}"
SIGN_IN_URL = f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={WEB_API_KEY}"

def signup_email_password(email: str, password: str) -> Dict:
    payload = {"email": email, "password": password, "returnSecureToken": True}
    r = requests.post(SIGN_UP_URL, json=payload, timeout=15)
    r.raise_for_status()
    return r.json()

def signin_email_password(email: str, password: str) -> Dict:
    payload = {"email": email, "password": password, "returnSecureToken": True}
    r = requests.post(SIGN_IN_URL, json=payload, timeout=15)
    r.raise_for_status()
    return r.json()

def verify_id_token(id_token: str):
    decoded = auth.verify_id_token(id_token)
    return decoded["uid"], decoded.get("email")

def ensure_user_doc(uid: str, email: str):
    ref = db.collection("users").document(uid)
    if not ref.get().exists:
        ref.set({
            "email": email,
            "plan": "free",
            "created_at": firestore.SERVER_TIMESTAMP,
            "prefs": {"stocks": [], "topics": [], "risk_tolerance": 2},
        })

def upsert_prefs(uid: str, stocks: List[str], topics: List[str], risk: int):
    db.collection("users").document(uid).set(
        {"prefs": {"stocks": stocks, "topics": topics, "risk_tolerance": risk}},
        merge=True,
    )

def signout():
    for k in ("id_token", "uid", "email"):
        st.session_state.pop(k, None)
    st.toast("로그아웃 완료", icon="✅")

# ========================
# Firestore fetchers
# ========================
@st.cache_data(show_spinner=False, ttl=60)
def fetch_generated(limit: int = 30) -> List[Dict]:
    """생성된 기사 우선(없으면 빈 리스트 반환). talks(신규) + 레거시(insights/actions) 함께 수집."""
    try:
        q = (db.collection("generated_articles_v3")
             .order_by("created_at", direction=firestore.Query.DESCENDING)
             .limit(limit))
        out = []
        for d in q.stream():
            x = d.to_dict() or {}
            out.append({
                "id": d.id,
                "title": x.get("title", "(제목 없음)"),
                "summary": x.get("summary", ""),
                "bullets": x.get("bullets", []),
                "evidence_urls": x.get("evidence_urls", []),
                "published_at": (x.get("published_window", {}) or {}).get("end", 0),
                "model": x.get("model", "n/a"),
                "talks": x.get("talks", {}),  # 신규 스키마
                # 레거시 호환
                "insights": x.get("insights", {"general": "", "entrepreneur": "", "politician": "", "investor": ""}),
                "actions": x.get("actions", {"general": [], "entrepreneur": [], "politician": [], "investor": []}),
                "__kind": "generated",
            })
        return out
    except Exception as e:
        st.error(f"생성 기사 로드 실패: {e}")
        return []

@st.cache_data(show_spinner=False, ttl=60)
def fetch_public(limit: int = 30) -> List[Dict]:
    """퍼블릭 기사(수동/테스트용). talks(신규) + 레거시 함께 수집."""
    try:
        q = (db.collection("public_articles")
             .order_by("published_at", direction=firestore.Query.DESCENDING)
             .limit(limit))
        out: List[Dict] = []
        for d in q.stream():
            x = d.to_dict() or {}
            out.append({
                "id": d.id,
                "title": x.get("title", "(제목 없음)"),
                "body_md": x.get("body_md", ""),
                "evidence_urls": x.get("evidence_urls", []),
                "source": x.get("source", ""),
                "published_at": x.get("published_at", 0),
                "talks": x.get("talks", {}),  # 신규 스키마
                # 레거시 호환
                "insights": x.get("insights", {"general": "", "entrepreneur": "", "politician": "", "investor": ""}),
                "actions": x.get("actions", {"general": [], "entrepreneur": [], "politician": [], "investor": []}),
                "__kind": "public",
            })
        return out
    except Exception as e:
        st.error(f"퍼블릭 기사 로드 실패: {e}")
        return []

def ts_to_str(ts: int) -> str:
    try:
        return datetime.fromtimestamp(int(ts), UTC).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return "-"

# ========================
# LLM(옵션): talks 생성 (폴백용)
# ========================
def _safe_json_loads(s: str) -> Dict:
    try:
        return json.loads(s)
    except Exception:
        pass
    import re
    content2 = re.sub(r"^```(?:json)?\s*|\s*```$", "", s.strip(), flags=re.I | re.M)
    try:
        return json.loads(content2)
    except Exception:
        pass
    m = re.search(r"\{.*\}", s, flags=re.S)
    if m:
        return json.loads(m.group(0))
    raise ValueError("JSON parse failed")

def generate_talks(title: str, content: str) -> Dict:
    """
    talks 스키마:
    {
      "talks": {
        "general": "2~4문장 한국어 대화체",
        "entrepreneur": "...",
        "politician": "...",
        "investor": "..."
      }
    }
    """
    if not OPENAI_API_KEY:
        return {
            "talks": {
                "general": "이번 이슈는 우리 일상과도 닿아 있어요. 가볍게 의견을 나누되, 단정적 표현은 피하면 좋아요. 갈등을 키우기보다 로컬 이슈나 실질적 도움으로 시선을 돌려보면 좋겠어요.",
                "entrepreneur": "시장 반응이 예민할 수 있으니 메시지는 차분하게, 고객 인터뷰와 작은 실험으로 가설부터 검증해요. 리스크는 작게, 학습은 빠르게 가져가 봅시다.",
                "politician": "사실관계 확인과 균형 잡힌 메시지가 우선이에요. 지역 현안과 연결되는 대안부터 단계적으로 제시하면 불필요한 반발을 줄일 수 있어요.",
                "investor": "헤드라인보다 펀더멘털과 현금흐름을 먼저 보세요. 변동성은 분산과 포지션 조절로 관리하고, 정보가 더 쌓일 때까지는 관망도 선택지예요."
            }
        }
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        prompt = (
            "역할: 당신은 뉴스를 읽고 독자 유형별로 행동/전제/리스크/대안을 자연스럽게 녹여 "
            "‘대화체 한 문단(2~4문장, 한국어)’으로 말해주는 조언자입니다. "
            "투자/정책 자문이 아닌 해석·참고용 톤을 유지하고, 과도한 확정 표현은 피하세요.\n\n"
            f"[기사 제목]\n{title}\n\n"
            f"[내용(요약 허용, 1500자 내)]\n{content[:1500]}\n\n"
            "출력은 JSON 하나만, 스키마는 다음과 같습니다:\n"
            "{\n"
            '  "talks": {\n'
            '    "general": "string",\n'
            '    "entrepreneur": "string",\n'
            '    "politician": "string",\n'
            '    "investor": "string"\n'
            "  }\n"
            "}\n"
            "각 문단에는 (행동 제안 + 전제/맥락 + 리스크 유의 + 현실적 대안)을 자연스럽게 포함하세요. "
            "JSON 외의 텍스트(설명/코드블록/마크다운)는 절대 출력하지 마세요."
        )
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            response_format={"type": "json_object"},
        )
        data = _safe_json_loads(resp.choices[0].message.content)
        talks = data.get("talks", {})
        for k in ["general", "entrepreneur", "politician", "investor"]:
            talks.setdefault(k, "")
        return {"talks": talks}
    except Exception as e:
        st.warning(f"LLM 호출 실패(템플릿 사용): {e}")
        return {
            "talks": {
                "general": "이번 이슈는 우리 일상과도 닿아 있어요. 가볍게 의견을 나누되, 단정적 표현은 피하면 좋아요. 갈등을 키우기보다 로컬 이슈나 실질적 도움으로 시선을 돌려보면 좋겠어요.",
                "entrepreneur": "시장 반응이 예민할 수 있으니 메시지는 차분하게, 고객 인터뷰와 작은 실험으로 가설부터 검증해요. 리스크는 작게, 학습은 빠르게 가져가 봅시다.",
                "politician": "사실관계 확인과 균형 잡힌 메시지가 우선이에요. 지역 현안과 연결되는 대안부터 단계적으로 제시하면 불필요한 반발을 줄일 수 있어요.",
                "investor": "헤드라인보다 펀더멘털과 현금흐름을 먼저 보세요. 변동성은 분산과 포지션 조절로 관리하고, 정보가 더 쌓일 때까지는 관망도 선택지예요."
            }
        }

# ========================
# 레거시(insights/actions) → talks 합성(Fallback)
# ========================
def compose_talk_from_legacy(insight: str, item: Dict) -> str:
    action = (item or {}).get("action", "").strip()
    assumptions = (item or {}).get("assumptions", "").strip()
    risk = (item or {}).get("risk", "").strip()
    alt = (item or {}).get("alternative", "").strip()

    parts = []
    if insight:
        parts.append(f"{insight.strip()} ")
    if action:
        parts.append(f"이번에는 '{action}'을(를) 가볍게 시도해보는 것도 좋아요. ")
    if assumptions:
        parts.append(f"다만 이 제안은 '{assumptions}' 같은 전제 위에서 더 힘을 발휘해요. ")
    if risk:
        parts.append(f"그리고 '{risk}' 부분은 미리 유의해 주세요. ")
    if alt:
        parts.append(f"상황에 따라 '{alt}' 같은 우회로도 현실적인 대안이 될 수 있어요.")
    text = "".join(parts).strip()
    if not text:
        text = "이 이슈는 단정짓기보다 상황을 넓게 살피는 편이 좋아요. 작게 시작해 보고, 위험 신호가 보이면 조정하는 접근을 권해요."
    return text

def build_talks_from_legacy(a: Dict) -> Dict:
    talks = {}
    reader_types = ["general", "entrepreneur", "politician", "investor"]
    for rt in reader_types:
        insight = (a.get("insights") or {}).get(rt, "")
        items = (a.get("actions") or {}).get(rt, [])
        talks[rt] = compose_talk_from_legacy(insight, items[0] if items else {})
    return talks

def save_talks_to_doc(kind: str, doc_id: str, talks: Dict):
    """talks를 문서에 병합 저장."""
    try:
        if kind == "generated":
            db.collection("generated_articles_v3").document(doc_id).set({"talks": talks}, merge=True)
        elif kind == "public":
            # 퍼블릭에도 저장하려면 주석 해제:
            # db.collection("public_articles").document(doc_id).set({"talks": talks}, merge=True)
            pass
    except Exception as e:
        st.warning(f"talks 저장 실패: {e}")

# ========================
# 기사 카드 렌더링
# ========================
def render_article_card(a: Dict, variant: str = "grid"):
    title_cls = "article-title"
    if variant == "hero":
        title_cls += " hero-title"
    elif variant == "side":
        title_cls += " side-title"

    with st.container():  # <- 부모 컨테이너
        # 부모에 카드 스타일이 적용되도록 마커만 출력
        st.markdown('<div class="itr-card-marker"></div>', unsafe_allow_html=True)

        # 이하 내용은 그대로
        st.markdown(f'<div class="{title_cls}">{a.get("title","(제목 없음)")}</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="article-meta">{ts_to_str(a.get("published_at", 0))}</div>', unsafe_allow_html=True)

        summary = a.get("summary") or a.get("body_md") or ""
        if summary:
            st.markdown(f'<div class="article-summary">{summary}</div>', unsafe_allow_html=True)

        bullets = a.get("bullets", [])
        if bullets:
            st.markdown('<div class="article-section-title">핵심 포인트</div>', unsafe_allow_html=True)
            for b in bullets:
                st.markdown(f"- {b}")

        evidence = a.get("evidence_urls", [])
        if evidence:
            st.markdown('<div class="article-section-title">출처</div>', unsafe_allow_html=True)
            for url in evidence:
                st.write(f"- [{url}]({url})")

        # talks 렌더링 로직은 그대로
        talks = a.get("talks") or {}
        if not any(talks.values() if isinstance(talks, dict) else []):
            has_legacy = any((a.get("insights") or {}).values()) or any((a.get("actions") or {}).values())
            if has_legacy:
                talks = build_talks_from_legacy(a)
            else:
                data = generate_talks(a.get("title",""), summary)
                talks = data.get("talks", {})
            if a.get("__kind") in ("generated", "public"):
                save_talks_to_doc(a["__kind"], a["id"], talks)
            a["talks"] = talks

        with st.expander("Itriggr는 이런 액션을 할 것 같아요", expanded=False):
            for rt in ["general", "entrepreneur", "politician", "investor"]:
                text = (talks or {}).get(rt, "").strip()
                if not text:
                    continue
                with st.chat_message("assistant"):
                    st.markdown(f"**{rt.capitalize()} 유형에게:**")
                    st.write(text)


# ========================
# 레이아웃 엔진 (히어로 + 사이드 + 3열 그리드 반복)
# ========================
HERO_ANCHOR = "one_plus_5k"  # 5의 배수번째 + 1번째 히어로

def compute_reserved_indices(n: int):
    """0-based 인덱스 sets: hero_set, side_set"""
    # 1, 6, 11...번째가 히어로 → 0, 5, 10... 인덱스
    hero_set = set(range(0, n, 5))
    # 히어로 다음 글은 사이드
    side_set = set(i + 1 for i in hero_set if i + 1 < n)
    return hero_set, side_set

def render_feed_with_layout(articles: List[Dict]):
    n = len(articles)
    if n == 0:
        return
    hero_set, side_set = compute_reserved_indices(n)

    i = 0
    while i < n:
        if i in hero_set:
            # ---- Hero + Side (중앙 컨테이너 안에서 8:4 비율) ----
            outer = st.columns([1, 12, 1], gap="large")
            with outer[1]:
                st.markdown('<div class="itr-row-start"></div>', unsafe_allow_html=True)
                inner = st.columns([8, 4], gap="large")
                with inner[0]:
                    render_article_card(articles[i], variant="hero")
                if i + 1 < n and (i + 1) in side_set:
                    with inner[1]:
                        render_article_card(articles[i + 1], variant="side")
                    i += 2
                else:
                    i += 1

            # ---- 아래 3열 그리드(다음 히어로/사이드 예약 인덱스 전까지 채움) ----
            slots = []
            j = i
            while j < n and len(slots) < 3:
                if (j in hero_set) or (j in side_set):
                    break
                slots.append(j)
                j += 1
            if slots:
                cols = st.columns(len(slots), gap="large")
                for idx, col in zip(slots, cols):
                    with col:
                        render_article_card(articles[idx], variant="grid")
                i = j
            continue

        # ---- 일반 3열 그리드 패킹 ----
        slots = []
        j = i
        while j < n and len(slots) < 3:
            if (j in hero_set) or (j in side_set):
                break
            slots.append(j)
            j += 1
        if slots:
            cols = st.columns(len(slots), gap="large")
            for idx, col in zip(slots, cols):
                with col:
                    render_article_card(articles[idx], variant="grid")
            i = j
        else:
            i += 1

# ========================
# 사이드바: 가입/로그인 유지
# ========================
with st.sidebar:
    st.header("🔐 계정")
    if "uid" not in st.session_state:
        tab_login, tab_signup = st.tabs(["로그인", "회원가입"])

        with tab_login:
            email = st.text_input("이메일", key="li_email")
            pw = st.text_input("비밀번호", type="password", key="li_pw")
            if st.button("로그인"):
                try:
                    res = signin_email_password(email, pw)
                    uid, verified_email = verify_id_token(res["idToken"])
                    st.session_state["id_token"] = res["idToken"]
                    st.session_state["uid"] = uid
                    st.session_state["email"] = verified_email or email
                    ensure_user_doc(uid, st.session_state["email"])
                    st.success("로그인 성공")
                    st.rerun()
                except requests.HTTPError as e:
                    msg = e.response.json().get("error", {}).get("message", str(e))
                    st.error(f"로그인 실패: {msg}")
                except Exception as e:
                    st.error(f"오류: {e}")

        with tab_signup:
            st.caption("비밀번호는 6자 이상")
            email = st.text_input("이메일", key="su_email")
            pw = st.text_input("비밀번호", type="password", key="su_pw")
            if st.button("회원가입"):
                try:
                    res = signup_email_password(email, pw)
                    st.success("회원가입 성공! 로그인 탭에서 로그인하세요.")
                    st.code(f"가입 이메일: {res['email']}", language="text")
                except requests.HTTPError as e:
                    msg = e.response.json().get("error", {}).get("message", str(e))
                    st.error(f"회원가입 실패: {msg}")
                except Exception as e:
                    st.error(f"오류: {e}")
    else:
        st.success(f"로그인됨: {st.session_state['email']}")
        if st.button("로그아웃"):
            signout()
            st.rerun()

        with st.expander("내 개인화 설정 (향후 사용)"):
            doc = db.collection("users").document(st.session_state["uid"]).get()
            prefs = (doc.to_dict() or {}).get("prefs", {}) if doc.exists else {}
            stocks = st.text_input("보유/관심 종목(쉼표구분)", value=",".join(prefs.get("stocks", [])))
            topics = st.text_input("관심 토픽(쉼표구분)", value=",".join(prefs.get("topics", [])))
            risk = st.slider("위험 성향", 1, 5, int(prefs.get("risk_tolerance", 2)))
            if st.button("저장"):
                try:
                    stocks_list = [s.strip() for s in stocks.split(",") if s.strip()]
                    topics_list = [t.strip() for t in topics.split(",") if t.strip()]
                    upsert_prefs(st.session_state["uid"], stocks_list, topics_list, risk)
                    st.toast("저장 완료", icon="✅")
                except Exception as e:
                    st.error(f"저장 실패: {e}")

# ========================
# 메인: 생성기사 우선 표시 (커스텀 레이아웃)
# ========================
st.title("📰 ITRiggr - 뉴스 피드")

gen = fetch_generated(limit=30)
articles = gen if gen else fetch_public(limit=30)

if gen:
    st.success("데이터 소스: generated_articles_v3")
else:
    st.warning("데이터 소스: public_articles (생성 기사가 아직 없거나 필터에 걸리지 않음)")

if not articles:
    st.info("표시할 기사가 없습니다. 잠시 후 다시 시도하거나 파이프라인 실행을 확인해 주세요.")
else:
    render_feed_with_layout(articles)
