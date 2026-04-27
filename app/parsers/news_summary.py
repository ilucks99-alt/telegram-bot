from typing import List

from app.logger import get_logger
from app.parsers import render_prompt
from app.services import gemini
from app.util import KST

logger = get_logger(__name__)


def _format_articles(articles: List[dict]) -> str:
    lines = []
    for i, a in enumerate(articles, 1):
        ts = a["published_at"].astimezone(KST).strftime("%m-%d %H:%M")
        kw = a.get("keyword", "")
        prefix = f"[{kw}] " if kw else ""
        lines.append(f"{i}. {prefix}{a['title']} ({a.get('source','')}, {ts})")
    return "\n".join(lines)


def summarize_news(query: str, articles: List[dict]) -> str:
    if not gemini.is_available():
        return "Gemini 미설정"
    if not articles:
        return f"검색 결과가 없습니다.\n검색어: {query}"

    prompt = render_prompt(
        "news_summarizer.txt",
        query=query,
        articles=_format_articles(articles),
    )
    text = gemini.generate_text(prompt, max_output_tokens=1500, temperature=0.3)
    return text or f"요약 생성에 실패했습니다.\n검색어: {query}"
