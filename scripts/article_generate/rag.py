import os
from typing import Dict
from langchain_community.document_loaders import WikipediaLoader
from langchain_community.vectorstores import FAISS
from common import normalize

# --- 환경 변수 ---
USE_RAG = os.getenv("USE_RAG", "False").lower() == "true"

def augment_with_wiki(item: Dict) -> Dict:
    """위키피디아 RAG로 기사에 컨텍스트 추가"""
    if not USE_RAG:
        item["wiki_context"] = ""
        return item

    try:
        # 의존성 임포트 (런타임 시 로드)
        import spacy
        # from langchain.vectorstores import FAISS
        nlp = spacy.load("en_core_web_sm", disable=["ner"])
        doc = nlp(item.get("content", "") + " " + item.get("content_hint", ""))
        entities = [ent.text for ent in doc.ents if ent.label_ == "ORG"][:3]  # 최대 3개 기업

        wiki_docs = []
        for entity in entities:
            try:
                loader = WikipediaLoader(query=entity, load_max_docs=1, doc_content_chars_max=500)
                docs = loader.load()
                wiki_docs.extend([normalize(doc.page_content)[:500] for doc in docs])
            except Exception:
                continue

        item["wiki_context"] = "\n".join(wiki_docs) if wiki_docs else ""
        return item
    except Exception as e:
        print(f"[RAG] Error: {e}")
        item["wiki_context"] = ""
        return item

if __name__ == "__main__":
    # 테스트용
    sample_item = {
        "content": "Tesla revealed a new battery technology...",
        "content_hint": "Tesla announces breakthrough in battery efficiency."
    }
    augmented = augment_with_wiki(sample_item)
    print(f"Wiki Context: {augmented['wiki_context']}")
