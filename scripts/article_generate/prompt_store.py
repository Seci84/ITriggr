# 프롬프트 관리자 (도서관 사서 같은 역할)
# “어느 폴더(v2025-09-02) 안에 있는 YAML 파일을 열어서 PromptTemplate로 변환해줘” 같은 기능 담당.
# 덕분에 앱 코드는 프롬프트 내용이 뭔지 몰라도, prompts.as_prompt("summary")만 호출하면 됨.


from __future__ import annotations

import os
import yaml
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Any, Optional

from langchain_core.prompts import PromptTemplate


# -------------------------
# Dataclass & Exceptions
# -------------------------
@dataclass(frozen=True)
class PromptSpec:
    name: str
    template: str
    input_vars: List[str]
    rel_path: str  # prompts/ 기준 상대경로 (e.g., "schema/v2025-09-02/summary.yaml")


class ManifestError(RuntimeError):
    pass


class PromptFileError(RuntimeError):
    pass


# -------------------------
# Registry
# -------------------------
class PromptRegistry:
    """
    Git에 저장된 프롬프트(YAML)를 manifest.yaml 기반으로 로드/관리하는 레지스트리.

    구조(사용자 현재 구성 예):
      scripts/article_generate/prompts/
        ├─ manifest.yaml
        └─ schema/
           ├─ final_json.yaml
           └─ v2025-09-02/
              ├─ summary.yaml
              ├─ bullets.yaml
              ├─ facts.yaml
              ├─ talks_general.yaml
              ├─ ...
              └─ (etc)

    - manifest.yaml의 'version_dir'과 'prompts[].out'을 신뢰 원천으로 사용.
    - name 키로 접근할 때는 'out' 파일명의 stem(확장자 제거)을 기본 키로 삼음.
      예: out: schema/v2025-09-02/summary.yaml  → name 키: "summary"
    """

    def __init__(
        self,
        prompts_root: Optional[str | Path] = None,
        version_dir_override: Optional[str] = None,
        manifest_filename: str = "manifest.yaml",
    ):
        # prompts_root 기본값: 현재 파일 기준 ../prompts
        self.prompts_root = Path(prompts_root) if prompts_root else Path(__file__).parent / "prompts"
        self.manifest_path = self.prompts_root / manifest_filename

        if not self.manifest_path.exists():
            raise ManifestError(f"manifest.yaml not found: {self.manifest_path}")

        self._manifest = self._load_yaml(self.manifest_path)

        # version_dir은 (1) 함수 인자 > (2) ENV > (3) manifest 순서로 결정
        env_override = os.getenv("PROMPTS_VERSION_DIR")
        self.version_dir = (
            version_dir_override
            or env_override
            or self._manifest.get("version_dir")
        )
        if not self.version_dir:
            raise ManifestError("manifest.yaml must contain 'version_dir' or provide override/ENV.")

        # id/out 목록 검증
        if "prompts" not in self._manifest or not isinstance(self._manifest["prompts"], list):
            raise ManifestError("manifest.yaml must contain 'prompts' list.")

        # 이름(파일 stem) → 상대경로 인덱스 생성
        # 예: "summary" → "schema/v2025-09-02/summary.yaml"
        self._index_by_name: Dict[str, str] = {}
        self._index_by_id: Dict[str, str] = {}
        for item in self._manifest["prompts"]:
            _id = item.get("id")
            out = item.get("out")
            if not _id or not out:
                raise ManifestError("Each prompt entry must contain 'id' and 'out' fields.")
            stem = Path(out).stem  # "summary.yaml" -> "summary"
            # 이름 충돌 방지: 같은 stem이 여러 번 나오면 에러
            if stem in self._index_by_name and self._index_by_name[stem] != out:
                raise ManifestError(f"Duplicate prompt name stem detected: '{stem}' -> {out}")
            self._index_by_name[stem] = out
            self._index_by_id[_id] = out

        # 간단 캐시
        self._spec_cache: Dict[str, PromptSpec] = {}

    # ---------- Public API ----------
    def read_manifest_version(self) -> str:
        """manifest 기준 현재 활성 버전 디렉토리(상대경로) 반환. 예: 'schema/v2025-09-02'"""
        return str(self.version_dir)

    def list_names(self) -> List[str]:
        """등록된 이름 키(stem) 목록"""
        return sorted(self._index_by_name.keys())

    def list_ids(self) -> List[str]:
        """등록된 LangSmith prompt id 목록(org/name:tag)"""
        return sorted(self._index_by_id.keys())

    def has_prompt(self, name_or_id: str) -> bool:
        return name_or_id in self._index_by_name or name_or_id in self._index_by_id

    def get_spec(self, name_or_id: str) -> PromptSpec:
        """
        name(=파일 stem) 또는 LangSmith id로 PromptSpec 반환.
        - 우선 name으로 조회, 없으면 id로 조회.
        """
        key = self._resolve_key(name_or_id)
        if key in self._spec_cache:
            return self._spec_cache[key]

        rel_path = self._resolve_rel_path(name_or_id)
        spec = self._load_prompt_spec(rel_path)
        self._spec_cache[key] = spec
        return spec

    def as_prompt(self, name_or_id: str) -> PromptTemplate:
        """LangChain PromptTemplate 반환."""
        spec = self.get_spec(name_or_id)
        return PromptTemplate(template=spec.template, input_variables=spec.input_vars)

    def get_path(self, name_or_id: str) -> Path:
        """프롬프트 YAML의 절대경로 반환 (디버깅/로그용)"""
        rel_path = self._resolve_rel_path(name_or_id)
        return self.prompts_root / rel_path

    # ---------- Internal ----------
    def _resolve_key(self, name_or_id: str) -> str:
        if name_or_id in self._index_by_name:
            return name_or_id
        if name_or_id in self._index_by_id:
            return name_or_id
        # name으로 들어왔는데 stem 매칭 실패시, 파일명에 .yaml 붙여본다(유연성)
        stem = Path(name_or_id).stem
        if stem in self._index_by_name:
            return stem
        raise PromptFileError(f"Prompt not found in manifest: {name_or_id}")

    def _resolve_rel_path(self, name_or_id: str) -> str:
        if name_or_id in self._index_by_name:
            return self._index_by_name[name_or_id]
        if name_or_id in self._index_by_id:
            return self._index_by_id[name_or_id]
        stem = Path(name_or_id).stem
        if stem in self._index_by_name:
            return self._index_by_name[stem]
        raise PromptFileError(f"Prompt not found in manifest: {name_or_id}")

    def _load_yaml(self, path: Path) -> Dict[str, Any]:
        try:
            return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except Exception as e:
            raise PromptFileError(f"Failed to read YAML: {path}\n{e}") from e

    def _load_prompt_spec(self, rel_path: str) -> PromptSpec:
        abs_path = self.prompts_root / rel_path
        if not abs_path.exists():
            raise PromptFileError(f"Prompt file does not exist: {abs_path}")

        data = self._load_yaml(abs_path)
        template = data.get("template")
        if not template:
            raise PromptFileError(f"'template' missing in {abs_path}")

        input_vars = data.get("input_vars", [])
        if not isinstance(input_vars, list):
            raise PromptFileError(f"'input_vars' must be a list in {abs_path}")

        name = data.get("name", Path(rel_path).stem)
        return PromptSpec(
            name=name,
            template=template,
            input_vars=input_vars,
            rel_path=rel_path,
        )
