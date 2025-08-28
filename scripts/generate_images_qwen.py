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
from typing import Any, Dict, Optional
from datetime import datetime, timezone

# Firebase
import firebase_admin
from firebase_admin import credentials, firestore, storage
from google.cloud.firestore_v1 import Transaction

# Image & client
from PIL import Image
from gradio_client import Client


# =========================
# 환경변수
# =========================
FIREBASE_CREDENTIALS_JSON = os.getenv("FIREBASE_CREDENTIALS_JSON", "")
FIREBASE_STORAGE_BUCKET   = os.getenv("FIREBASE_STORAGE_BUCKET", "")
QWEN_SPACE                = os.getenv("QWEN_SPACE", "Qwen/Qwen-Image")
RUN_LIMIT                 = int(os.getenv("RUN_LIMIT", "30"))

# Qwen infer 기본 파라미터(필요 시 조정)
QWEN_ARGS = {
    "seed": 0,
    "randomize_seed": True,
    "aspect_ratio": "16:9",
    "guidance_scale": 4,
    "num_inference_steps": 50,
    "prompt_enhance": True,
    "api_name": "/infer",  # Space API 탭에서 확인 가능
}

# 저장 규격(카드용 16:9)
UPLOAD_WIDTH  = 960
UPLOAD_HEIGHT = 540


# =========================
# Firebase 초기화
# =========================
def init_firebase():
    if not firebase_admin._apps:
        if not FIREBASE_CREDENTIALS_JSON or not os.path.exists(FIREBASE_CREDENTIALS_JSON):
            raise RuntimeError("Set FIREBASE_CREDENTIALS_JSON and FIREBASE_STORAGE_BUCKET")
        cred = credentials.Certificate(FIREBASE_CREDENTIALS_JSON)
        firebase_admin.initialize_app(cred, {"storageBucket": FIREBASE_STORAGE_BUCKET})
    return firestore.client(), storage.bucket(FIREBASE_STORAGE_BUCKET)


# =========================
# Prompt 빌드
# =========================
def build_prompt_from_article(a: Dict[str, Any], reader_type: str = "general") -> str:
    """
    summary + (선택) talks[reader_type]를 섞어 '신문 일러스트' 톤으로 요청.
    너무 길면 잘라 안정화.
    """
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
    """
    Qwen/Qwen-Image Space를 Gradio Client로 호출하여 로컬 임시경로를 받는다.
    그 파일을 열어 960x540 webp로 변환해 bytes 반환.
    """
    client = Client(QWEN_SPACE)
    result = client.predict(prompt=prompt, **QWEN_ARGS)

    # 결과가 tuple/list이면 첫 요소가 로컬 경로인 경우가 많음
    path = result[0] if isinstance(result, (list, tuple)) else str(result)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Image not found at: {path}")

    with Image.open(path) as im:
        # 색상 모드 정리
        if im.mode not in ("RGB", "RGBA"):
            im = im.convert("RGBA")
        # 리사이즈
        im = im.resize((UPLOAD_WIDTH, UPLOAD_HEIGHT), Image.LANCZOS)

        out = io.BytesIO()
        im.save(out, format="WEBP", quality=92, method=6)
        return out.getvalue()


# =========================
# Storage 업로드
# =========================
def upload_image_bytes_to_firebase(img_bytes: bytes, dest_path: str, content_type: str = "image/webp") -> str:
    """
    Firebase Storage에 업로드하고 토큰 URL 반환.
    """
    bucket = storage.bucket()
    blob = bucket.blob(dest_path)
    token = str(uuid.uuid4())

    blob.upload_from_string(img_bytes, content_type=content_type)
    blob.metadata = {"firebaseStorageDownloadTokens": token}
    blob.patch()

    quoted = dest_path.replace("/", "%2F")
    return f"https://firebasestorage.googleapis.com/v0/b/{bucket.name}/o/{quoted}?alt=media&token={token}"


# =========================
# 중복 생성 방지: 트랜잭션 락
# =========================
def article_lock_or_skip(db: firestore.Client, doc_ref: firestore.DocumentReference) -> bool:
    """
    트랜잭션으로 image_status를 'pending'으로 설정하여 선점.
    이미 hero가 있거나 status=done/pending이면 False(스킵).
    """
    @firestore.transactional
    def _tx(tx: Transaction) -> bool:
        snap = tx.get(doc_ref)
        data = snap.to_dict() or {}
        images_map = data.get("images_map") or {}
        status = (data.get("image_status") or "").lower()

        if images_map.get("hero") or status in ("pending", "done"):
            return False

        tx.update(doc_ref, {
            "image_status": "pending",
            "image_lock_at": firestore.SERVER_TIMESTAMP
        })
        return True

    return _tx(db.transaction())


# =========================
# 문서별 생성/저장
# =========================
def ensure_image_for_article(doc_id: str, a: Dict[str, Any], db: firestore.Client) -> Optional[Dict[str, Any]]:
    doc_ref = db.collection("generated_articles_v3").document(doc_id)

    # 0) 이미 있는지 빠른 체크(낙관)
    if (a.get("images_map") or {}).get("hero"):
        return None

    # 1) 동시성 가드: pending 선점
    if not article_lock_or_skip(db, doc_ref):
        return None  # 이미 생성됨 / 다른 워커가 작업 중

    try:
        # 2) 이미지 생성
        prompt = build_prompt_from_article(a, reader_type="general")
        img_bytes = qwen_generate_image(prompt)

        # 3) 업로드 (문서ID 기반 경로)
        ts = int(time.time())
        dest = f"articles/{doc_id}/hero_{ts}.webp"
        url = upload_image_bytes_to_firebase(img_bytes, dest)

        # 4) 메타 기록(문서ID 포함)
        hero_record = {
            "article_id": doc_id,
            "kind": "hero",
            "url": url,
            "prompt": prompt,
            "meta": {
                "backend": "gradio_client",
                "model": QWEN_SPACE,
                "w": UPLOAD_WIDTH,
                "h": UPLOAD_HEIGHT
            },
            "created_at": firestore.SERVER_TIMESTAMP
        }

        # 5) 원문 문서 갱신
        doc_ref.set({
            "images_map": {"hero": hero_record},
            "image_status": "done",
            "image_updated_at": firestore.SERVER_TIMESTAMP
        }, merge=True)

        # 6) 별도 컬렉션(감사/조회용): 문서ID 고정
        db.collection("generated_images").document(f"{doc_id}_hero").set(hero_record, merge=True)

        return hero_record

    except Exception as e:
        # 실패 시 상태 업데이트(추후 재시도 가능)
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

    created = 0
    for snap in q.stream():
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
