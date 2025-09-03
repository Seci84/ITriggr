# LangSmith Hub ↔ Git 연결 다리
# Hub에서 마음에 드는 버전에 태그(2025-09-02)를 찍으면 → 이 스크립트가 해당 프롬프트들을 YAML 파일로 내려받아 prompts/v2025-09-02/에 저장.
# LangSmith에서 실험한 결과물을 Git 레포에 “스냅샷”으로 남기는 도구.

# scripts/article_generate/pull_prompts.py
from __future__ import annotations
import os
import yaml
from pathlib import Path
from typing import List, Dict, Any

from langsmith import Client

PROMPTS_ROOT = Path(__file__).parent / "prompts"
MANIFEST_PATH = PROMPTS_ROOT / "manifest.yaml"

def load_manifest() -> Dict[str, Any]:
    if not MANIFEST_PATH.exists():
        raise FileNotFoundError(f"manifest.yaml not found at: {MANIFEST_PATH}")
    return yaml.safe_load(MANIFEST_PATH.read_text(encoding="utf-8"))

def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

def serialize_prompt_to_yaml(prompt_obj) -> Dict[str, Any]:
    """
    LangSmith에서 가져온 Prompt 객체를 YAML(dict)로 직렬화.
    - template / input_variables가 있으면 그대로 사용
    - ChatPromptTemplate(messages) 형태면 간단한 텍스트 템플릿으로 평탄화
    """
    # 1) template 필드가 있는 경우 (PromptTemplate 계열)
    template = getattr(prompt_obj, "template", None)
    input_vars = list(getattr(prompt_obj, "input_variables", []) or [])

    if template and isinstance(template, str):
        return {
            "name": getattr(prompt_obj, "name", None) or "prompt",
            "input_vars": input_vars,
            "template": template,
        }

    # 2) ChatPromptTemplate인 경우 messages 평탄화
    messages = getattr(prompt_obj, "messages", None)
    if messages:
        lines: List[str] = []
        # LangChain 메시지 프롬프트 객체들의 구조를 단순화
        for m in messages:
            role = getattr(m, "role", None) or getattr(m, "type", "human")
            # MessagePromptTemplate이면 .prompt.template 가 있을 수 있음
            content = getattr(m, "template", None)
            if content is None:
                prompt_inner = getattr(m, "prompt", None)
                content = getattr(prompt_inner, "template", None) if prompt_inner else None
            if content is None:
                content = str(m)
            lines.append(f"{role.upper()}:\n{content}")
        flat_template = "\n\n".join(lines)
        if not input_vars:
            input_vars = list(getattr(prompt_obj, "input_variables", []) or [])
        return {
            "name": getattr(prompt_obj, "name", None) or "chat_prompt",
            "input_vars": input_vars,
            "template": flat_template,
        }

    # 3) 비정형: 가능한 속성들을 조사
    # 최후 보루: 객체 전체를 문자열화
    return {
        "name": getattr(prompt_obj, "name", None) or "unknown_prompt",
        "input_vars": input_vars,
        "template": str(prompt_obj),
    }

def main():
    api_key = os.getenv("LANGSMITH_API_KEY")
    if not api_key:
        raise EnvironmentError("LANGSMITH_API_KEY is not set.")

    manifest = load_manifest()
    version_dir = manifest.get("version_dir")
    items = manifest.get("prompts", [])
    if not version_dir or not isinstance(items, list):
        raise ValueError("manifest.yaml must contain 'version_dir' and 'prompts' list.")

    client = Client()  # uses LANGSMITH_API_KEY

    saved = []
    for p in items:
        pid = p.get("id")     # e.g., Personal/news-summary:2025-09-02
        out = p.get("out")    # e.g., schema/v2025-09-02/summary.yaml
        if not pid or not out:
            raise ValueError("Each prompt entry must have 'id' and 'out'.")

        # LangSmith에서 pull
        prompt_obj = client.pull_prompt(pid)
        data = serialize_prompt_to_yaml(prompt_obj)

        # 로컬에 저장
        out_path = PROMPTS_ROOT / out
        ensure_parent(out_path)
        out_path.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")
        saved.append(str(out_path.relative_to(PROMPTS_ROOT)))

    print("✅ Pulled prompts:")
    for s in saved:
        print(" -", s)

if __name__ == "__main__":
    main()
