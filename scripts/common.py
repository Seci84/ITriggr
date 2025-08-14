import os, re, json, time, hashlib
import firebase_admin
from firebase_admin import credentials, firestore
from dateutil import parser as dtparser
from datetime import datetime, timezone
import hashlib

# --- Firestore init ---

def doc_id_from_url(url: str) -> str:
    """URL을 SHA256으로 32자 고정 ID로 변환 (raw_articles 문서 ID로 사용)."""
    return hashlib.sha256((url or "").encode("utf-8")).hexdigest()[:32]

def init_db():
    svc = os.getenv("FIREBASE_SERVICE_ACCOUNT")
    if not svc:
        raise RuntimeError("FIREBASE_SERVICE_ACCOUNT not set")
    try:
        data = json.loads(svc)  # JSON 문자열로 들어온 경우
        cred = credentials.Certificate(data)
    except json.JSONDecodeError:
        # 파일 경로로 들어온 경우 (드물지만 대비)
        cred = credentials.Certificate(svc)
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    return firestore.client()

# --- time utils ---
def now_epoch():
    return int(time.time())

def to_epoch(x, default=None):
    try:
        return int(dtparser.parse(x).timestamp())
    except Exception:
        return default if default is not None else now_epoch()

# --- text / hash / simhash ---
def normalize(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "")).strip()

def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def simhash(text: str, bits: int = 64) -> str:
    # 아주 가벼운 simhash (토큰 단위)
    toks = re.findall(r"[A-Za-z0-9가-힣]+", (text or "").lower())
    if not toks:
        return "0" * (bits // 4)
    v = [0] * bits
    for tok in toks:
        h = int(hashlib.md5(tok.encode()).hexdigest(), 16)
        for i in range(bits):
            v[i] += 1 if (h >> i) & 1 else -1
    out = 0
    for i in range(bits):
        if v[i] >= 0:
            out |= (1 << i)
    return f"{out:0{bits//4}x}"

def sim_prefix(simhash_hex: str, prefix_bits: int = 16) -> str:
    return simhash_hex[: prefix_bits // 4]

# --- logging ---
def log_event(db, kind: str, payload: dict):
    db.collection("logs_ingest").add(
        {"kind": kind, "payload": payload, "ts": firestore.SERVER_TIMESTAMP}
    )
