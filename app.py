import streamlit as st
import requests
import firebase_admin
from firebase_admin import credentials, auth, firestore
from datetime import datetime, timezone

st.set_page_config(page_title="ITRiggr - News (Hybrid)", page_icon="📰", layout="wide")

# ---------------------------
# Firebase Admin 초기화
# ---------------------------
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
        "client_x509_cert_url": st.secrets["FIREBASE_CLIENT_X509_CERT_URL"]
    })
    firebase_admin.initialize_app(cred)

db = firestore.client()
WEB_API_KEY = st.secrets.get("FIREBASE_API_KEY")
OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY")  # 선택: 있으면 LLM 사용

# ---------------------------
# Auth REST endpoints
# ---------------------------
SIGN_UP_URL = f"https://identitytoolkit.googleapis.com/v1/accounts:signUp?key={WEB_API_KEY}"
SIGN_IN_URL = f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={WEB_API_KEY}"

def signup_email_password(email: str, password: str):
    payload = {"email": email, "password": password, "returnSecureToken": True}
    r = requests.post(SIGN_UP_URL, json=payload, timeout=10)
    r.raise_for_status()
    return r.json()

def signin_email_password(email: str, password: str):
    payload = {"email": email, "password": password, "returnSecureToken": True}
    r = requests.post(SIGN_IN_URL, json=payload, timeout=10)
    r.raise_for_status()
    return r.json()

def verify_id_token(id_token: str):
    decoded = auth.verify_id_token(id_token)
    return decoded["uid"], decoded.get("email")

def ensure_user_doc(uid: str, email: str):
    ref = db.collection("users").document(uid)
    snap = ref.get()
    if not snap.exists:
        ref.set({
            "email": email,
            "plan": "free",
            "created_at": firestore.SERVER_TIMESTAMP,
            "prefs": {"stocks": [], "topics": [], "risk_tolerance": 2}
        })

def upsert_prefs(uid: str, stocks, topics, risk):
    db.collection("users").document(uid).set(
        {"prefs": {"stocks": stocks, "topics": topics, "risk_tolerance": risk}},
        merge=True
    )

def signout():
    for k in ("id_token", "uid", "email"):
        st.session_state.pop(k, None)
    st.toast("로그아웃 완료", icon="✅")

# ---------------------------
# Firestore helpers (공개 기사)
# ---------------------------
@st.cache_data(show_spinner=False, ttl=60)
def fetch_articles(limit: int = 30):
    try:
        q = (
            db.collection("public_articles")
            .order_by("published_at", direction=firestore.Query.DESCENDING)
            .limit(limit)
        )
        items = []
        for d in q.stream():
            data = d.to_dict() or {}
            items.append({
                "id": d.id,
                "title": data.get("title", "(제목 없음)"),
                "source": data.get("source", ""),
                "body_md": data.get("body_md", ""),
                "evidence_urls": data.get("evidence_urls", []),
                "published_at": data.get("published_at", 0),
            })
        return items
    except Exception as e:
        st.error(f"기사 목록 로드 실패: {e}")
        return []

def ts_to_str(ts):
    try:
        return datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return "-"

# ---------------------------
# 액션 제안 (LLM 연결 옵션)
# ---------------------------
def generate_actions(title: str, body: str):
    """
    OPENAI_API_KEY가 있으면 LLM으로 생성, 없으면 템플릿 반환.
    """
    if OPENAI_API_KEY:
        try:
            # OpenAI SDK 사용 시 requirements에 openai 추가 후 아래 주석 해제
            # from openai import OpenAI
            # client = OpenAI(api_key=OPENAI_API_KEY)
            # prompt = f"""
            # [기사 제목] {title}
            # [본문(요약 가능)] {body[:1500]}
            # (1)주식 (2)선물/파생 (3)비즈 의사결정 각각에 대해
            # 가능한 액션 / 전제 / 리스크 / 대안을 간결 JSON으로 제안.
            # 스키마: {{"stock":[{{"action":"","assumptions":"","risk":"","alternative":""}}], "futures":[...], "biz":[...]}}
            # '투자 자문 아님' 톤 유지, 과도한 확정 표현 금지.
            # """
            # resp = client.chat.completions.create(
            #     model="gpt-4o-mini",
            #     messages=[{"role":"user","content":prompt}],
            #     temperature=0.3
            # )
            # import json
            # return json.loads(resp.choices[0].message.content)
            pass
        except Exception as e:
            st.warning(f"LLM 호출 실패(템플릿 반환): {e}")

    # 템플릿 (LLM 미사용 시)
    return {
        "stock": [{
            "action": "관련 섹터/종목 워치리스트 등록 및 거래량 변화 모니터링",
            "assumptions": "기사 이슈가 단기 모멘텀에 영향 가능",
            "risk": "루머/해프닝 가능성, 단기 과열",
            "alternative": "공식 가이던스 전까지 분할 관찰/소액 접근"
        }],
        "futures": [{
            "action": "섹터 ETF로 소규모 탐색 포지션(엄격한 손절)",
            "assumptions": "섹터가 뉴스에 베타 반응",
            "risk": "거시 이벤트로 역방향 급변",
            "alternative": "옵션 스프레드 등 변동성 제한 전략"
        }],
        "biz": [{
            "action": "공급망/고객 공지 모니터링 및 리드타임/원가 재평가",
            "assumptions": "분기 내 가격·납기 반영 가능",
            "risk": "공식 확인 전 과잉 대응",
            "alternative": "2~3개 소스 교차 확인 후 단계적 적용"
        }]
    }

def show_actions_ui(actions: dict):
    st.subheader("🧭 액션 제안")
    cols = st.columns(3)
    blocks = [("📈 주식", "stock", cols[0]), ("📉 선물/파생", "futures", cols[1]), ("🏢 비즈니스", "biz", cols[2])]
    for title, key, col in blocks:
        with col:
            st.markdown(f"**{title}**")
            for a in actions.get(key, []):
                st.markdown(f"- **가능한 액션**: {a['action']}")
                st.caption(f"전제: {a['assumptions']} | 리스크: {a['risk']} | 대안: {a['alternative']}")

# ---------------------------
# 사이드바: 가입/로그인 UI (유지)
# ---------------------------
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

        # (선택) 개인화 prefs UI — 지금은 숨겨두고, 차후 활성화 가능
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

# ---------------------------
# 메인: 누구나 기사 전체 열람 가능
# ---------------------------
st.title("📰 ITRiggr - 퍼블릭 뉴스 피드")

articles = fetch_articles(limit=30)
if not articles:
    st.info("공개 기사(`public_articles`)가 아직 없습니다. Firestore 콘솔에서 문서를 추가해 보세요.")
else:
    # 좌측: 목록, 우측: 상세
    left, right = st.columns([1, 2], gap="large")

    with left:
        st.subheader("기사 목록")
        options = {f"{a['title']} ({a.get('source','')}) — {ts_to_str(a['published_at'])}": a["id"] for a in articles}
        selected = st.selectbox("열람할 기사를 선택하세요", options.keys())
        selected_id = options[selected]

    with right:
        # 선택한 기사 표시
        sel = next((a for a in articles if a["id"] == selected_id), None)
        if sel:
            st.subheader(sel["title"])
            meta = f"{sel.get('source','')} | {ts_to_str(sel['published_at'])}"
            st.caption(meta)
            st.write(sel["body_md"])

            if sel.get("evidence_urls"):
                with st.expander("근거 링크"):
                    for u in sel["evidence_urls"]:
                        st.markdown(f"- {u}")

            st.divider()
            if st.button("이 기사 기반 액션 제안 보기"):
                actions = generate_actions(sel["title"], sel["body_md"])
                show_actions_ui(actions)

st.markdown("---")
st.caption("ⓘ 본 서비스는 'AI 재구성/제안' 기능을 포함하며, 투자 자문이 아닙니다.")

