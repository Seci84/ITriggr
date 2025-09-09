# News Article Ingestion Pipeline

## Overview
Collects news from NewsAPI and RSS, identifies actionable articles (e.g., investment, M&A, product launches), and stores only relevant ones in Firestore. Designed for small teams to minimize manual effort and optimize data quality.

## Current Features
- **Sources**: NewsAPI (English headlines, max 50 articles) and RSS feeds (configurable).
- **Deduplication**: Removes exact duplicates via URL hashing.
- **Storage**: Saves metadata (title, URL, source, date, summary) to Firestore `raw_articles`.
- **Prep**: Generates `simhash` for future clustering.
- **Logging**: Tracks errors and stats in Firestore.

## Planned Improvements
To store only actionable articles, we skip manual keyword/rules-based filtering (too complex for small teams) and use **LLM-based analysis** for contextual actionability scoring.

### Phase 3: LLM-Based Filtering
#### Objective
Identify and store articles with actionable insights using an LLM, minimizing setup effort.

#### Pipeline
1. **Collection**: Fetch metadata from NewsAPI/RSS (existing).
2. **Content Crawling**: Extract article body with `newspaper3k` (2000-char limit, async with `aiohttp`).
3. **LLM Analysis**:
   - Use `gpt-4o-mini` with LangChain `PromptTemplate` to score (0-10) and explain actionability.
   - Example prompt:


- Store articles with score >=7.
4. **Storage**: Add Firestore fields: `content`, `action_score`, `action_reason`, `crawl_status`, `llm_processed_at`.
5. **LangSmith**: Trace LLM inputs/outputs for debugging.
6. **Logging**: Log skipped articles (score <7) in `skipped_articles`.

#### Small-Team Optimizations
- **No Manual Rules**: LLM handles contextual analysis.
- **Cost Control**: Limit to 50-100 articles/day, batch LLM calls.
- **Ease**: LangSmith free tier for monitoring.

#### Optional Future Enhancements
- **Wikipedia RAG**: Augment with Nasdaq-100 company data (LangChain `WikipediaLoader`, FAISS).
- **More Sources**: Add Yahoo Finance, SEC EDGAR.
- **Action Types**: Classify actions (e.g., investment vs. M&A).

## Implementation Plan
- **Week 1**: Add crawling, test LLM scoring, filter by score.
- **Weeks 2-3**: Add LangSmith, optimize prompts, batch processing.
- **Week 4+**: (Optional) Integrate Wikipedia RAG.

## Why This?
- **Accurate**: LLM ensures contextual relevance.
- **Efficient**: Stores 10-20% of articles, cuts costs.
- **Scalable**: Easy to add RAG or sources later.

## Dependencies
- `newsapi-python`, `feedparser`, `newspaper3k`, `langchain`, `openai`, `firebase-admin`.
- Optional: `spacy`, `faiss-cpu`.


# ITriggr
News App Project

# 버전 설명
- LangSmith 프롬프트를 계속 사용하되, LangChain 체인에 통합해서 RAG와 결합한 더 강력한 파이프라인 개발
- 인덱싱 대상: 외부 웹사이트(예: 위키피디아)
- 향후 생성된 뉴스 기사가 누적될 경우 내부 데이터와 결합하여 RAG 생성
- RAG로 크롤링/임베딩한 데이터를 LangSmith 프롬프트에 주입해 요약/talks 강화

# 인덱싱 범위 설정
- Wikipedia.search()로 키워드 검색 후 상위 5-10개 페이지 필터링
