import streamlit as st
import sys, os
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ARTICLE_DIR = os.path.join(BASE_DIR, "scripts", "article_generate")
if ARTICLE_DIR not in sys.path:
    sys.path.insert(0, ARTICLE_DIR)

import requests
import json
import firebase_admin
from firebase_admin import credentials, auth, firestore
from datetime import datetime, UTC
from typing import List, Dict
from openai import OpenAI

# Pipeline module imports
from news_pipeline_langchain import _llm, _hub, _format_prompt, build_with_hub_prompts, _str
from process_articles import build_actionability
from rag import augment_with_wiki

st.set_page_config(page_title="ITRiggr - News", page_icon="📰", layout="wide")

# ========================
# Global Styles (Journal-like title/body)
# ========================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@700&family=Merriweather:wght@400;700&display=swap');

.article-title {
  font-family: 'Playfair Display', serif;
  font-size: 1.8rem;
  line-height: 1.25;
  margin: 0.2rem 0 0.4rem 0;
}

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

.block-container {
  max-width: 800px;
  margin-left: auto;
  margin-right: auto;
  padding-left: 1rem;
  padding-right: 1rem;
}
</style>
""", unsafe_allow_html=True)

# ========================
# Firebase Admin Initialization
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
# Auth REST Endpoints
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
    st.toast("Logout successful", icon="✅")

# ========================
# Firestore Fetchers
# ========================
@st.cache_data(show_spinner=False, ttl=60)
def fetch_raw(limit: int = 30) -> List[Dict]:
    """Fetch recent articles from raw_articles_v6."""
    try:
        q = (db.collection("raw_articles_v6")
             .order_by("published_at", direction=firestore.Query.DESCENDING)
             .limit(limit))
        out = []
        for d in q.stream():
            x = d.to_dict() or {}
            out.append({
                "id": d.id,
                "title": x.get("title", "(No title)"),
                "url": x.get("url", ""),
                "published_at": x.get("published_at", 0),
                "content_hint": x.get("content_hint", ""),
                "__kind": "raw",
            })
        return out
    except Exception as e:
        st.error(f"Failed to load raw_articles_v6: {e}")
        return []

@st.cache_data(show_spinner=False, ttl=60)
def fetch_actionable(limit: int = 30) -> List[Dict]:
    """Fetch recent articles from actionable_articles."""
    try:
        q = (db.collection("actionable_articles")
             .order_by("llm_processed_at", direction=firestore.Query.DESCENDING)
             .limit(limit))
        out = []
        for d in q.stream():
            x = d.to_dict() or {}
            out.append({
                "id": d.id,
                "title": x.get("title", "(No title)"),
                "summary": x.get("content_hint", ""),
                "content": x.get("content", ""),
                "evidence_urls": [url] if url else [],
                # "evidence_urls": x.get("url", []),
                "published_at": x.get("published_at", 0),
                "action_score": x.get("action_score", 0),
                "talks": x.get("talks", {}),
                # 🔹 위키 컨텍스트 가져오기(있으면 표시용으로만 활용)
                "wiki_context": x.get("wiki_context", ""),
                "__kind": "actionable",
            })
        return out
    except Exception as e:
        st.error(f"Failed to load actionable_articles: {e}")
        return []


@st.cache_data(show_spinner=False, ttl=60)
def fetch_generated(limit: int = 30) -> List[Dict]:
    """Fetch generated articles first (returns empty list if none). Includes talks (new) + legacy."""
    try:
        q = (db.collection("generated_articles_v6")
             .order_by("created_at", direction=firestore.Query.DESCENDING)
             .limit(limit))
        out = []
        for d in q.stream():
            x = d.to_dict() or {}
            out.append({
                "id": d.id,
                "title": x.get("title", "(No title)"),
                "summary": x.get("summary", ""),
                "bullets": x.get("bullets", []),
                "evidence_urls": x.get("evidence_urls", []),
                "published_at": (x.get("published_window", {}) or {}).get("end", 0),
                "model": x.get("model", "n/a"),
                "talks": x.get("talks", {}),
                "images_map": x.get("images_map", {}),
                "images": x.get("images", []),
                "insights": x.get("insights", {"general": "", "entrepreneur": "", "politician": "", "investor": ""}),
                "actions": x.get("actions", {"general": [], "entrepreneur": [], "politician": [], "investor": []}),
                "__kind": "generated",
            })
        return out
    except Exception as e:
        st.error(f"Failed to load generated_articles_v6: {e}")
        return []

@st.cache_data(show_spinner=False, ttl=60)
def fetch_public(limit: int = 30) -> List[Dict]:
    """Fetch fallback articles from public_articles."""
    try:
        q = (db.collection("public_articles")
             .order_by("created_at", direction=firestore.Query.DESCENDING)
             .limit(limit))
        out = []
        for d in q.stream():
            x = d.to_dict() or {}
            out.append({
                "id": d.id,
                "title": x.get("title", "(No title)"),
                "summary": x.get("summary") or x.get("content_hint", "") or x.get("body_md", ""),
                "bullets": x.get("bullets", []),
                "evidence_urls": x.get("evidence_urls", [x.get("url", "")] if x.get("url") else []),
                "published_at": x.get("published_at", 0),
                "images_map": x.get("images_map", {}),
                "images": x.get("images", []),
                "talks": x.get("talks", {}),
                "__kind": "public",
            })
        return out
    except Exception as e:
        st.error(f"Failed to load public_articles: {e}")
        return []


def ts_to_str(ts: int) -> str:
    try:
        return datetime.fromtimestamp(int(ts), UTC).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return "-"

# ========================
# LLM: Generate Talks (Conversational Paragraph)
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
    if not OPENAI_API_KEY or not _llm:
        return {
            "talks": {
                "general": "This issue connects to our daily lives. Share opinions lightly, avoid definitive statements, and focus on local issues or practical help to reduce conflict.",
                "entrepreneur": "Market reactions might be sensitive, so keep messages calm, start with customer interviews and small experiments to test hypotheses. Keep risks low and learn quickly.",
                "politician": "Prioritize fact-checking and balanced messaging. Suggest solutions tied to local issues step-by-step to minimize unnecessary backlash.",
                "investor": "Focus on fundamentals and cash flow rather than headlines. Manage volatility with diversification and position adjustments, and consider waiting for more information."
            }
        }
    try:
        prompt = _format_prompt(_hub("talks_general"), input=content[:1500], title=title)
        talks = {}
        for role in ["general", "entrepreneur", "politician", "investor"]:
            talk_prompt = _format_prompt(_hub(f"talks_{role}"), input=content[:1500], title=title)
            talks[role] = (_llm | _str).invoke(talk_prompt).strip()
        return {"talks": talks}
    except Exception as e:
        st.warning(f"LLM call failed (using template): {e}")
        return {
            "talks": {
                "general": "This issue connects to our daily lives. Share opinions lightly, avoid definitive statements, and focus on local issues or practical help to reduce conflict.",
                "entrepreneur": "Market reactions might be sensitive, so keep messages calm, start with customer interviews and small experiments to test hypotheses. Keep risks low and learn quickly.",
                "politician": "Prioritize fact-checking and balanced messaging. Suggest solutions tied to local issues step-by-step to minimize unnecessary backlash.",
                "investor": "Focus on fundamentals and cash flow rather than headlines. Manage volatility with diversification and position adjustments, and consider waiting for more information."
            }
        }

# ========================
# Legacy to Conversational Synthesis (Fallback)
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
        parts.append(f"Consider lightly trying '{action}' this time. ")
    if assumptions:
        parts.append(f"However, this suggestion works best with premises like '{assumptions}'. ")
    if risk:
        parts.append(f"Be mindful of '{risk}' in advance. ")
    if alt:
        parts.append(f"Depending on the situation, '{alt}' could be a practical alternative. ")
    text = "".join(parts).strip()
    if not text:
        text = "It’s better to broadly assess this issue rather than jumping to conclusions. Start small and adjust if risk signals appear."
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
    try:
        if kind == "generated":
            db.collection("generated_articles_v6").document(doc_id).set({"talks": talks}, merge=True)
        elif kind == "actionable":
            db.collection("actionable_articles").document(doc_id).set({"talks": talks}, merge=True)
        # Uncomment to save to public articles if needed
    except Exception as e:
        st.warning(f"Failed to save talks: {e}")

# ========================
# Conversational UI (expander)
# ========================
def show_talks_ui(sel: Dict):
    talks = sel.get("talks", {}) or {}
    reader_types = ["general", "entrepreneur", "politician", "investor"]
    with st.expander("Itriggr suggests these actions", expanded=False):
        for rt in reader_types:
            text = talks.get(rt, "").strip()
            if not text:
                continue
            with st.chat_message("assistant"):
                st.markdown(f"**To {rt.capitalize()} type:**")
                st.write(text)

# ========================
# Sidebar: Sign-up/Login Persistence
# ========================
with st.sidebar:
    st.header("🔐 Account")
    if "uid" not in st.session_state:
        tab_login, tab_signup = st.tabs(["Login", "Sign Up"])

        with tab_login:
            email = st.text_input("Email", key="li_email")
            pw = st.text_input("Password", type="password", key="li_pw")
            if st.button("Login"):
                try:
                    res = signin_email_password(email, pw)
                    uid, verified_email = verify_id_token(res["idToken"])
                    st.session_state["id_token"] = res["idToken"]
                    st.session_state["uid"] = uid
                    st.session_state["email"] = verified_email or email
                    ensure_user_doc(uid, st.session_state["email"])
                    st.success("Login successful")
                    st.rerun()
                except requests.HTTPError as e:
                    msg = e.response.json().get("error", {}).get("message", str(e))
                    st.error(f"Login failed: {msg}")
                except Exception as e:
                    st.error(f"Error: {e}")

        with tab_signup:
            st.caption("Password must be at least 6 characters")
            email = st.text_input("Email", key="su_email")
            pw = st.text_input("Password", type="password", key="su_pw")
            if st.button("Sign Up"):
                try:
                    res = signup_email_password(email, pw)
                    st.success("Sign-up successful! Please log in on the Login tab.")
                    st.code(f"Registered email: {res['email']}", language="text")
                except requests.HTTPError as e:
                    msg = e.response.json().get("error", {}).get("message", str(e))
                    st.error(f"Sign-up failed: {msg}")
                except Exception as e:
                    st.error(f"Error: {e}")
    else:
        st.success(f"Logged in: {st.session_state['email']}")
        if st.button("Logout"):
            signout()
            st.rerun()

        with st.expander("My Personalization Settings (Future Use)"):
            doc = db.collection("users").document(st.session_state["uid"]).get()
            prefs = (doc.to_dict() or {}).get("prefs", {}) if doc.exists else {}
            stocks = st.text_input("Held/Interested Stocks (comma-separated)", value=",".join(prefs.get("stocks", [])))
            topics = st.text_input("Interested Topics (comma-separated)", value=",".join(prefs.get("topics", [])))
            risk = st.slider("Risk Tolerance", 1, 5, int(prefs.get("risk_tolerance", 2)))
            if st.button("Save"):
                try:
                    stocks_list = [s.strip() for s in stocks.split(",") if s.strip()]
                    topics_list = [t.strip() for t in topics.split(",") if t.strip()]
                    upsert_prefs(st.session_state["uid"], stocks_list, topics_list, risk)
                    st.toast("Save successful", icon="✅")
                except Exception as e:
                    st.error(f"Save failed: {e}")

# ========================
# Main: Article Display (raw, actionable, generated priority)
# ========================
st.title("📰 ITRiggr - News Feed")

# User preference-based filtering (future expansion)
uid = st.session_state.get("uid")
if uid:
    doc = db.collection("users").document(uid).get()
    prefs = (doc.to_dict() or {}).get("prefs", {}) if doc.exists else {}
    topics = prefs.get("topics", [])
    filter_topic = st.selectbox("Interest Topic Filter", ["All"] + topics) if topics else "All"

raw = fetch_raw(limit=10)
actionable = fetch_actionable(limit=10)
generated = fetch_generated(limit=10)
articles = generated or actionable or raw or fetch_public(limit=30)

if generated:
    st.success("Data source: generated_articles_v6")
elif actionable:
    st.success("Data source: actionable_articles")
elif raw:
    st.success("Data source: raw_articles_v6")
else:
    st.warning("Data source: public_articles (no generated articles or filtered out)")

if not articles:
    st.info("No articles to display. Please try again later or check the pipeline execution.")
else:
    st.subheader("Article List")
    for a in articles:
        # ---- Title (always displayed) ----
        st.markdown(
            f"<div class='article-title'>{a.get('title','(No title)')}</div>",
            unsafe_allow_html=True
        )

        # ---- Image: Use only URLs stored in Firestore (right below headline) ----
        hero_url = None
        if a.get("__kind") == "generated":
            hero = (a.get("images_map") or {}).get("hero")
            if isinstance(hero, dict):
                hero_url = hero.get("url")
        elif a.get("images"):
            hero_url = a.get("images", [{}])[0].get("url")

        if hero_url:
            st.image(hero_url, use_column_width=True)

        # ---- Meta Info (below image) ----
        st.markdown(
            f"<div class='article-meta'>{ts_to_str(a.get('published_at', 0))}</div>",
            unsafe_allow_html=True
        )

        # ---- Summary/Body (always displayed) ----
        summary = a.get("summary") or a.get("content_hint") or a.get("body_md") or ""
        if summary:
            st.markdown(f"<div class='article-summary'>{summary}</div>", unsafe_allow_html=True)

        # ---- Key Points (always displayed) ----
        bullets = a.get("bullets", [])
        if bullets:
            st.markdown("<div class='article-section-title'>Key Points</div>", unsafe_allow_html=True)
            for b in bullets:
                st.markdown(f"- {b}")

        # ---- Sources (always displayed) ----
        evidence = a.get("evidence_urls", [a.get("url", "")] if a.get("url") else [])
        if evidence:
            st.markdown("<div class='article-section-title'>Sources</div>", unsafe_allow_html=True)
            for url in evidence:
                st.write(f"- [{url}]({url})")

        # Wikipedia context (있을 때만 표시)
        wiki_ctx = a.get("wiki_context", "")
        if wiki_ctx:
            with st.expander("Wikipedia context", expanded=False):
                st.write(wiki_ctx)


        # ---- Talks Preparation: Use DB talks first, then legacy/LLM generation ----
        talks = a.get("talks") or {}
        newly_generated = False

        if not any(talks.values()) and a.get("__kind") in ["actionable", "generated"]:
            has_legacy = any((a.get("insights") or {}).values()) or any((a.get("actions") or {}).values())
            if has_legacy:
                talks = build_talks_from_legacy(a)
                newly_generated = True
            else:
                content = a.get("content") or a.get("summary") or a.get("content_hint") or ""
                talks_data = generate_talks(a.get("title", ""), content)
                talks = talks_data.get("talks", {})
                newly_generated = True

            if newly_generated:
                save_talks_to_doc(a["__kind"], a["id"], talks)
            a["talks"] = talks

        # ---- Conversational UI (expander) ----
        show_talks_ui(a)

        st.divider()
