# generate_images_qwen.py
"""
generated_articles_v3의 summary를 프롬프트로 사용해
Qwen/Qwen-Image(Gradio Client)로 이미지를 생성하고,
Firebase Storage에 업로드한 뒤,
원문 문서에 images_map.hero로 저장합니다.

- GPU 불필요(원격 Space 호출)
- 중복 생성 방지: 트랜잭션으로 image_status=pending 선점
- 성공 시: image_status=done, images_map.hero 저장
"""

import os
import io
import time
import uuid
import json
import tempfile
from typing import Any, Dict, Optional

# Firebase
import firebase_admin
from firebase_admin import credentials, firestore, storage
from google.cloud.firestore_v1 import Transaction

# Image & client
from PIL import Image
from gradio_client import Client

from google.cloud import exceptions as gcloud_exceptions



# =========================
# 환경변수
# =========================
FIREBASE_CREDENTIALS_JSON = os.getenv("FIREBASE_SERVICE_ACCOUNT", "")
QWEN_SPACE                = os.getenv("QWEN_SPACE", "Qwen/Qwen-Image")
RUN_LIMIT                 = int(os.getenv("RUN_LIMIT", "30"))

# Qwen infer 기본 파라미터
QWEN_ARGS = {
    "seed": 0,
    "randomize_seed": True,
    "aspect_ratio": "16:9",
    "guidance_scale": 4,
    "num_inference_steps": 50,
    "prompt_enhance": True,
    "api_name": "/infer",
}

# 저장 규격(카드용 16:9)
UPLOAD_WIDTH  = 960
UPLOAD_HEIGHT = 540


# =========================
# Firebase 초기화 (경로 or JSON 문자열 지원, 버킷 자동 추출)
# =========================

FIREBASE_BUCKET_NAME = os.getenv("FIREBASE_STORAGE_BUCKET", "streamlit-test-d4ef0.firebasestorage.app")

def init_firebase():
    if not firebase_admin._apps:
        if not FIREBASE_CREDENTIALS_JSON:
            raise RuntimeError("Set FIREBASE_CREDENTIALS_JSON (or FIREBASE_SERVICE_ACCOUNT)")

        cred = None

        if os.path.exists(FIREBASE_CREDENTIALS_JSON):
            # 파일 경로
            cred = credentials.Certificate(FIREBASE_CREDENTIALS_JSON)
        else:
            # JSON 문자열
            try:
                data = json.loads(FIREBASE_CREDENTIALS_JSON)
                tmpfile = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
                tmpfile.write(json.dumps(data).encode("utf-8"))
                tmpfile.flush()
                cred = credentials.Certificate(tmpfile.name)
            except Exception as e:
                raise RuntimeError("FIREBASE_CREDENTIALS_JSON is neither a valid path nor valid JSON") from e

        # ✅ 명시적으로 버킷 이름 지정
        firebase_admin.initialize_app(cred, {"storageBucket": FIREBASE_BUCKET_NAME})

    return firestore.client(), storage.bucket(FIREBASE_BUCKET_NAME)



# =========================
# Prompt 빌드
# =========================
def build_prompt_from_article(a: Dict[str, Any], reader_type: str = "general") -> str:
    summary = (a.get("summary") or "").strip()[:300]
    talks = ((a.get("talks") or {}).get(reader_type) or "").strip()[:160]
    style = (
        "newspaper editorial illustration, flat minimal, high contrast, vector shading, "
        "no text, clean shapes, soft rim light, cinematic composition"
    )
    return f"{style}. Scene inspired by: {summary}. Hint: {talks}" if talks else f"{style}. Scene inspired by: {summary}"


# =========================
# Qwen 이미지 생성 (bytes)
# =========================
def qwen_generate_image(prompt: str) -> bytes:
    client = Client(QWEN_SPACE)
    result = client.predict(prompt=prompt, **QWEN_ARGS)

    path = result[0] if isinstance(result, (list, tuple)) else str(result)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Image not found at: {path}")

    with Image.open(path) as im:
        if im.mode not in ("RGB", "RGBA"):
            im = im.convert("RGBA")
        im = im.resize((UPLOAD_WIDTH, UPLOAD_HEIGHT), Image.LANCZOS)

        out = io.BytesIO()
        im.save(out, format="WEBP", quality=92, method=6)
        return out.getvalue()


# =========================
# Storage 업로드 (custom metadata 포함)
# =========================
def upload_image_bytes_to_firebase(
    img_bytes: bytes,
    dest_path: str,
    content_type: str = "image/webp",
    extra_metadata: Optional[Dict[str, str]] = None,   # 👈 추가 메타데이터
) -> str:
    bucket = storage.bucket()
    blob = bucket.blob(dest_path)
    token = str(uuid.uuid4())

    blob.upload_from_string(img_bytes, content_type=content_type)

    # 기본 토큰 + 사용자 정의 메타데이터 병합 (모두 문자열)
    md = {"firebaseStorageDownloadTokens": token}
    if extra_metadata:
        md.update({k: str(v) for k, v in extra_metadata.items()})

    blob.metadata = md
    blob.patch()

    quoted = dest_path.replace("/", "%2F")
    return f"https://firebasestorage.googleapis.com/v0/b/{bucket.name}/o/{quoted}?alt=media&token={token}"


# =========================
# 중복 생성 방지: 트랜잭션 락
# =========================

from google.cloud import exceptions as gcloud_exceptions

def article_lock_or_skip(db: firestore.Client, doc_ref: firestore.DocumentReference) -> bool:
    transaction = db.transaction()

    @firestore.transactional
    def _lock(tx: firestore.Transaction, ref: firestore.DocumentReference) -> bool:
        snap = ref.get(transaction=tx)   # ✅ 트랜잭션-세이프하게 스냅샷 읽기
        if not hasattr(snap, "to_dict"):
            print(f"[LOCK-SKIP] unexpected snapshot type: {type(snap)}")
            return False

        data = snap.to_dict() or {}
        images_map = data.get("images_map") or {}
        status = (data.get("image_status") or "").lower()

        # 이미 작업 중/완료면 스킵
        if images_map.get("hero") or status in ("pending", "done"):
            return False

        # 선점 플래그 기록 (트랜잭션 안에서)
        tx.update(ref, {
            "image_status": "pending",
            "image_lock_at": firestore.SERVER_TIMESTAMP
        })
        return True

    try:
        # ✅ 트랜잭션 함수 실행(커밋/재시도는 SDK가 처리)
        return _lock(transaction, doc_ref)
    except gcloud_exceptions.Aborted:
        # 경쟁으로 트랜잭션이 중단되면 스킵 처리
        return False
    except Exception as e:
        print(f"[LOCK-ERR] {doc_ref.id}: {e}")
        return False


# =========================
# 문서별 생성/저장
# =========================
def ensure_image_for_article(doc_id: str, a: Dict[str, Any], db: firestore.Client) -> Optional[Dict[str, Any]]:
    doc_ref = db.collection("generated_articles_v3").document(doc_id)

    # 빠른 스킵
    if (a.get("images_map") or {}).get("hero"):
        return None
    if not article_lock_or_skip(db, doc_ref):
        return None

    try:
        # 1) 이미지 생성
        prompt = build_prompt_from_article(a, reader_type="general")
        img_bytes = qwen_generate_image(prompt)

        # 2) 업로드 (문서ID 기반 경로 + 메타데이터에 article_id 포함)
        ts = int(time.time())
        dest = f"articles/{doc_id}/hero_{ts}.webp"
        url = upload_image_bytes_to_firebase(
            img_bytes,
            dest,
            extra_metadata={
                "article_id": doc_id,                       # ← 문서 ID
                "collection": "generated_articles_v3",
                "kind": "hero",
            },
        )

        # 3) Firestore 기록 (문서 및 별도 컬렉션)
        hero_record = {
            "article_id": doc_id,
            "kind": "hero",
            "url": url,
            "prompt": prompt,
            "meta": {
                "backend": "gradio_client",
                "model": QWEN_SPACE,
                "w": UPLOAD_WIDTH,
                "h": UPLOAD_HEIGHT,
                "bucket": storage.bucket().name,
                "path": dest,
                "article_id": doc_id,
            },
            "created_at": firestore.SERVER_TIMESTAMP
        }

        doc_ref.set({
            "images_map": {"hero": hero_record},
            "image_status": "done",
            "image_updated_at": firestore.SERVER_TIMESTAMP
        }, merge=True)

        db.collection("generated_images").document(f"{doc_id}_hero").set(hero_record, merge=True)

        return hero_record

    except Exception as e:
        doc_ref.set({
            "image_status": "failed",
            "image_error": str(e),
            "image_failed_at": firestore.SERVER_TIMESTAMP
        }, merge=True)
        raise



# =========================
# 메인
# =========================
def run(limit: int = RUN_LIMIT):
    db, _bucket = init_firebase()

    q = (db.collection("generated_articles_v3")
           .order_by("created_at", direction=firestore.Query.DESCENDING)
           .limit(limit))

    docs = q.get()   # ✅ stream() 대신 get()

    created = 0
    for snap in docs:
        if not hasattr(snap, "to_dict"):
            print(f"[SKIP] unexpected type: {type(snap)}")
            continue

        doc_id = snap.id
        a = snap.to_dict() or {}
        try:
            rec = ensure_image_for_article(doc_id, a, db)
            if rec:
                created += 1
                print(f"[OK] {doc_id} → {rec['url']}")
            else:
                print(f"[SKIP] {doc_id} (exists/locked/done)")
        except Exception as e:
            print(f"[ERR] {doc_id}: {e}")

    print(f"Done. images_created={created}")



if __name__ == "__main__":
    run()
