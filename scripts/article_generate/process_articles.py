import os
import asyncio
import aiohttp
from typing import List, Dict, Optional
from newspaper import Article
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import JsonOutputParser
from langsmith import Client
from firebase_admin import firestore
from common import normalize, log_event
from rag import augment_with_wiki

# --- 환경 변수 ---
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
USE_OPENAI = os.getenv("USE_OPENAI", "False").lower() == "true"
LANGSMITH_API_KEY = os.getenv("LANGSMITH_API_KEY", "")

# --- LLM 및 LangSmith 설정 ---
_llm = None
if OPENAI_API_KEY and USE_OPENAI:
    try:
        _llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.2)
        print("✅ OpenAI client initialized")
    except Exception as e:
        print(f"❌ OpenAI client init failed: {e}")
        USE_OPENAI = False

if LANGSMITH_API_KEY:
    os.environ["LANGCHAIN_TRACING_V2"] = "true"
    os.environ["LANGCHAIN_API_KEY"] = LANGSMITH_API_KEY
    os.environ.setdefault("LANGCHAIN_PROJECT", os.getenv("LANGSMITH_PROJECT", "news-pipeline"))

_ls = Client()
_json = JsonOutputParser()

# --- LangSmith 프롬프트 ID ---
PROMPT_IDS = {
    "actionability": "actionability:2025-09-09"
}

def _hub(name: str):
    """LangSmith에서 프롬프트 로드, 실패 시 :latest 폴백"""
    pid = PROMPT_IDS.get(name, f"{name}:latest")
    try:
        return _ls.pull_prompt(pid)
    except Exception:
        base = pid.split(":", 1)[0]
        print(f"[Hub] Falling back to latest for {base}")
        return _ls.pull_prompt(f"{base}:latest")

def _format_prompt(p, **vals):
    """프롬프트 변수 정규화"""
    def norm_key(k: str) -> str:
        return (k or "").strip().strip('"').strip("'")

    input_vars_raw = getattr(p, "input_variables", []) or []
    iv_pairs = [(v, norm_key(v)) for v in input_vars_raw]

    if "input" in vals:
        for alias in ("text", "content", "article", "body"):
            vals.setdefault(alias, vals["input"])
            vals.setdefault(f'"{alias}"', vals[alias])

    for v_raw, v_norm in iv_pairs:
        if v_norm in vals and v_raw not in vals:
            vals[v_raw] = vals[v_norm]
        if v_raw in vals and v_norm not in vals:
            vals[v_norm] = vals[v_raw]
        if v_raw not in vals and v_norm not in vals:
            print(f"[WARNING] Missing variable {v_raw}, setting to empty string")
            vals[v_norm] = ""
            vals[v_raw] = ""

    while True:
        try:
            return p.format(**vals)
        except KeyError as e:
            missing_raw = str(e).strip()
            missing_norm = norm_key(missing_raw)
            print(f"[WARNING] KeyError for {missing_raw}, setting to empty string")
            vals[missing_raw] = ""
            vals[missing_norm] = vals[missing_raw]

async def fetch_content(url: str) -> str:
    """비동기 본문 크롤링"""
    try:
        article = Article(url)
        article.download()
        article.parse()
        content = normalize(article.text)[:500]
        return content or "Content unavailable"
    except Exception as e:
        print(f"Failed to fetch content from {url}: {e}")
        return "Content unavailable"

async def fetch_contents(items: List[Dict]) -> List[Dict]:
    """비동기 배치 크롤링"""
    async with aiohttp.ClientSession() as session:
        tasks = []
        for item in items:
            tasks.append(asyncio.ensure_future(fetch_content(item["url"])))
        contents = await asyncio.gather(*tasks, return_exceptions=True)
        for item, content in zip(items, contents):
            item["content"] = str(content) if not isinstance(content, Exception) else "Content unavailable"
            item["crawl_status"] = "success" if not isinstance(content, Exception) else "failed"

            # 위키 컨텍스트 주입 (USE_RAG이 False면 빈 문자열을 반환)
            try:
                aug = augment_with_wiki({
                    "content": item.get("content", ""),
                    "content_hint": item.get("content_hint", "")
                })
                item["wiki_context"] = aug.get("wiki_context", "")
            except Exception:
                item["wiki_context"] = ""
        return items


def build_actionability(item: Dict) -> Optional[Dict]:
    """LLM으로 액션 가능성 판단"""
    if not _llm or not USE_OPENAI:
        return None
    try:
        input_text = f"{item['title']}\n{item.get('content_hint', '')}\n{item.get('content', '')}"
        prompt = _hub("actionability")
        result = (_llm | _json).invoke(_format_prompt(
            prompt,
            input=input_text,
            title=item["title"],
            content_hint=item.get("content_hint", ""),
            content=item.get("content", "")
        ))
        if not isinstance(result, dict) or "score" not in result or "reason" not in result:
            print(f"[WARNING] Invalid LLM output: {result}")
            return None
        return result
    except Exception as e:
        print(f"[Actionability] Error: {e}")
        return None

def save_filtered(db, items: List[Dict]):
    """LLM 점수 >=7인 기사만 저장"""
    saved, skipped = 0, 0
    col = db.collection("actionable_articles")

    for item in items:
        result = build_actionability(item)
        if not result:
            log_event(db, "skipped_article", {
                "url": item["url"],
                "reason": "LLM failed or disabled"
            })
            skipped += 1
            continue

        score = result.get("score", 0)
        reason = result.get("reason", "")
        if score < 7:
            log_event(db, "skipped_article", {
                "url": item["url"],
                "score": score,
                "reason": reason
            })
            skipped += 1
            continue

        doc = {
            "source": item["source"],
            "source_name": item["source_name"],
            "title": item["title"],
            "url": item["url"],
            "published_at": item["published_at"],
            "content_hint": item.get("content_hint", ""),
            "content": item.get("content", ""),
            "lang": item.get("lang", "en"),
            "url_hash": item["url_hash"],
            "simhash": item["simhash"],
            "action_score": score,
            "action_reason": reason,
            "crawl_status": item.get("crawl_status", "unknown"),
            # 위키 컨텍스트 저장(스키마에 영향 없이 선택 필드로 추가)
            "wiki_context": item.get("wiki_context", ""),
            "llm_processed_at": firestore.SERVER_TIMESTAMP,
            "created_at": firestore.SERVER_TIMESTAMP
        }
        col.document(item["url_hash"]).set(doc)
        saved += 1

    log_event(db, "filter_done", {"saved": saved, "skipped": skipped, "total": len(items)})
    print(f"Saved={saved}, Skipped={skipped}, Total={len(items)}")
    return saved, skipped

async def analyze_and_save(db, items: List[Dict]):
    """메인 처리 함수"""
    items = await fetch_contents(items[:100])  # 하루 100개 제한
    saved, skipped = save_filtered(db, items)
    return saved, skipped

# --- RAG (옵션, 주석 처리) ---
# def augment_with_wiki(item: Dict) -> Dict:
#     import spacy
#     from langchain_community.document_loaders import WikipediaLoader
#     from langchain.vectorstores import FAISS
#     nlp = spacy.load("en_core_web_sm")
#     doc = nlp(item.get("content", "") + " " + item.get("content_hint", ""))
#     entities = [ent.text for ent in doc.ents if ent.label_ == "ORG"]
#     wiki_docs = []
#     for entity in entities[:3]:  # 최대 3개 기업
#         try:
#             loader = WikipediaLoader(query=entity, load_max_docs=1)
#             docs = loader.load()
#             wiki_docs.extend([normalize(doc.page_content)[:500] for doc in docs])
#         except:
#             continue
#     item["wiki_context"] = "\n".join(wiki_docs) if wiki_docs else ""
#     return item

if __name__ == "__main__":
    from common import init_db
    db = init_db()
    sample_items = [{
        "source": "newsapi",
        "source_name": "Test News",
        "title": "Test Article",
        "url": "https://example.com",
        "published_at": 1631234567,
        "content_hint": "Test summary",
        "lang": "en",
        "url_hash": "abc123",
        "simhash": "def456"
    }]
    asyncio.run(analyze_and_save(db, sample_items))
