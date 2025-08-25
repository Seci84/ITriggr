# 기사 목록을 위 아래로 나열 (최신 상단, order_by("created_at", DESCENDING) 유지).
# 각 기사를 st.expander로 구현: 타이틀 클릭 시 확장, 요약, 핵심 포인트, 출처 표시.
# 별도 페이지 대신 expander 사용 (단일 파일 유지, 필요 시 multi-page 확장 가능).
# 제목: "Itriggr는 이런 액션을 할 것 같아요"로 변경.
# reader_types 순서: general, entrepreneur, politician, investor.
# 채팅창 스타일: st.chat_message("assistant")로 말풍선 형태로 출력.
# insights: caption으로 표시.
# actions: markdown으로 액션 나열.

import streamlit as st
import requests
import json
import firebase_admin
from firebase_admin import credentials, auth, firestore
from datetime import datetime, UTC  # DeprecationWarning 처리
from typing import List, Dict
from openai import OpenAI  # LLM 호출 활성화

st.set_page_config(page_title="ITRiggr - News", page_icon="📰", layout="wide")

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
OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY")  # 선택

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
    """생성된 기사 우선(없으면 빈 리스트 반환)."""
    try:
        q = (db.collection("generated_articles")
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
                "insights": x.get("insights", {"general": "", "entrepreneur": "", "politician": "", "investor": ""}),  # 추가
                "actions": x.get("actions", {"general": [], "entrepreneur": [], "politician": [], "investor": []}),  # 추가
                "__kind": "generated",
            })
        return out
    except Exception as e:
        st.error(f"생성 기사 로드 실패: {e}")
        return []

@st.cache_data(show_spinner=False, ttl=60)
def fetch_public(limit: int = 30) -> List[Dict]:
    """퍼블릭 기사(수동/테스트용)"""
    try:
        q = (db.collection("public_articles")
             .order_by("published_at", direction=firestore.Query.DESCENDING)
             .limit(limit))
        out = []
        for d in q.stream():
            x = d.to_dict() or {}
            out.append({
                "id": d.id,
                "title": x.get("title", "(제목 없음)"),
                "body_md": x.get("body_md", ""),
                "evidence_urls": x.get("evidence_urls", []),
                "source": x.get("source", ""),
                "published_at": x.get("published_at", 0),
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
# 액션 제안 (LLM 선택)
# ========================
def generate_actions(title: str, content: str) -> Dict:
    """OPENAI_API_KEY가 있으면 LLM, 없으면 템플릿."""
    if OPENAI_API_KEY:
        try:
            client = OpenAI(api_key=OPENAI_API_KEY)
            prompt = (
                f"[기사 제목]\n{title}\n\n[내용(요약 허용)]\n{content[:1500]}\n\n"
                "주식/선물/비즈 각각에 대해 액션, 전제, 리스크, 대안을 간결 JSON으로:"
                ' {"stock":[{"action":"","assumptions":"","risk":"","alternative":""}],'
                '  "futures":[...], "biz":[...]}'
                " 투자 자문 아님 톤, 과도한 확정 표현 금지."
            )
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3
            )
            return json.loads(resp.choices[0].message.content)
        except Exception as e:
            st.warning(f"LLM 호출 실패(템플릿 사용): {e}")

    # 템플릿(LLM 미사용 시)
    return {
        "insights": {
            "general": "Career opportunities: Astronautical Engineer roles at SpaceX",
            "entrepreneur": "Patent companies: Quantum Technologies",
            "politician": "Related laws: Space Policy Directives",
            "investor": "Challenged companies: Boeing facing management issues"
        },
        "actions": {
            "general": [{
                "action": "Learn quantum computing via Coursera for roles like Astronautical Engineer at SpaceX",
                "assumptions": "Tech advancement creates jobs",
                "risk": "Skill obsolescence",
                "alternative": "Join community forums"
            }],
            "entrepreneur": [{
                "action": "Explore partnerships with Quantum Technologies for laser comm patents",
                "assumptions": "Patent holders open for M&A",
                "risk": "Access restrictions",
                "alternative": "R&D investment"
            }],
            "politician": [{
                "action": "Strengthen Space Policy Directives for quantum navigation",
                "assumptions": "Gaps in international accords",
                "risk": "International disputes",
                "alternative": "Congressional hearings"
            }],
            "investor": [{
                "action": "Invest in ARKX ETF for SpaceX exposure",
                "assumptions": "Chained opportunities from Space Force missions",
                "risk": "Management issues in ULA",
                "alternative": "Diversified space funds"
            }]
        }
    }

def show_actions_ui(sel: Dict):
    st.subheader("Itriggr는 이런 액션을 할 것 같아요")
    actions = sel.get("actions", {})
    insights = sel.get("insights", {})
    reader_types = ["general", "entrepreneur", "politician", "investor"]
    for reader_type in reader_types:
        with st.chat_message("assistant"):  # 채팅창처럼 말풍선 형태
            st.markdown(f"**{reader_type.capitalize()} 유형에게:**")
            st.caption(insights.get(reader_type, "No insights available"))
            for a in actions.get(reader_type, []):
                st.markdown(f"- **액션**: {a['action']}")
                st.caption(f"전제: {a['assumptions']} | 리스크: {a['risk']} | 대안: {a['alternative']}")

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
# 메인: 생성기사 우선 표시
# ========================
st.title("📰 ITRiggr - 뉴스 피드")

gen = fetch_generated(limit=30)
articles = gen if gen else fetch_public(limit=30)

if gen:
    st.success("데이터 소스: generated_articles")
else:
    st.warning("데이터 소스: public_articles (생성 기사가 아직 없거나 필터에 걸리지 않음)")

if not articles:
    st.info("표시할 기사가 없습니다. 잠시 후 다시 시도하거나 파이프라인 실행을 확인해 주세요.")
else:
    st.subheader("기사 목록")
    for a in articles:
        with st.expander(a["title"]):
            st.caption(ts_to_str(a.get("published_at", 0)))
            st.write(a.get("summary", ""))
            bullets = a.get("bullets", [])
            if bullets:
                st.markdown("**핵심 포인트:**")
                for b in bullets:
                    st.markdown(f"- {b}")
            if a.get("evidence_urls"):
                st.markdown("**출처:**")
                for url in a["evidence_urls"]:
                    st.write(f"- [{url}]({url})")
            # 액션 제안
            show_actions_ui(a)
