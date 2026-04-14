from datetime import datetime
from typing import Any, Dict, List

from app import config
from app.db_engine import InvestmentDB
from app.logger import get_logger
from app.parsers.news_summary import (
    extract_entities_from_news,
    match_portfolio_impact,
    summarize_news,
)
from app.services.news_rss import search_google_news_rss
from app.services.telegram import send_long_message, send_message
from app.util import KST

logger = get_logger(__name__)


def handle_news_search_command(chat_id, raw: str) -> None:
    query = raw.replace("/검색", "", 1).strip()
    if not query:
        send_message(chat_id, "검색어를 같이 입력해주세요.\n예: /검색 오늘의 국내 주식시장")
        return

    try:
        articles = search_google_news_rss(query, limit=10)
        if not articles:
            send_message(chat_id, f"검색 결과가 없습니다.\n검색어: {query}")
            return

        summary = summarize_news(query, articles)
        send_long_message(chat_id, summary)
    except Exception:
        logger.exception("news search failed")
        send_message(chat_id, "뉴스 검색 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.")


def collect_news_for_keywords() -> List[Dict[str, Any]]:
    all_items: List[Dict[str, Any]] = []
    seen = set()

    for kw in config.NEWS_KEYWORDS[:10]:
        try:
            items = search_google_news_rss(kw, limit=config.NEWS_PER_KEYWORD_LIMIT)
            for item in items:
                key = (item["title"].lower(), item.get("source", ""))
                if key in seen:
                    continue
                seen.add(key)
                item["keyword"] = kw
                all_items.append(item)
        except Exception:
            logger.exception("뉴스 수집 실패 | keyword=%s", kw)

    all_items.sort(key=lambda x: x["published_at"], reverse=True)
    return all_items[:config.NEWS_REPORT_MAX_ARTICLES]


def _matches_scheduled_slot() -> bool:
    """KST 기준으로 현재 시각이 NEWS_REPORT_TIMES 중 하나와 ±15분 이내인지."""
    now = datetime.now(KST)
    today = now.strftime("%Y-%m-%d")
    for t in config.NEWS_REPORT_TIMES:
        try:
            base = datetime.strptime(f"{today} {t}", "%Y-%m-%d %H:%M").replace(tzinfo=KST)
        except ValueError:
            continue
        if abs((now - base).total_seconds()) <= 900:
            return True
    return False


def run_scheduled_news_report(db: InvestmentDB, chat_id) -> str:
    """
    GitHub Actions cron 호출 시 실행. 성공 시 "ok", 슬롯 아니면 "skipped".
    뉴스→포트폴리오 임팩트 매칭을 포함.
    """
    if not config.NEWS_AUTO_REPORT_ENABLED:
        return "disabled"

    if not _matches_scheduled_slot():
        return "skipped"

    try:
        news_items = collect_news_for_keywords()
        if not news_items:
            send_message(chat_id, "신규 뉴스 없음")
            return "empty"

        # Entity extraction + portfolio impact
        portfolio_impact = []
        try:
            entities = extract_entities_from_news(news_items)
            if entities:
                portfolio_impact = match_portfolio_impact(entities, db)
        except Exception:
            logger.exception("portfolio impact matching failed; falling back to plain summary")

        summary = summarize_news("자동 뉴스 보고", news_items, portfolio_impact=portfolio_impact)

        slot = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
        report = f"📰 뉴스 자동 보고 ({slot})\n\n{summary}\n\n[수집 기사 {len(news_items)}건]\n"
        for i, item in enumerate(news_items[:10], 1):
            report += f"\n{i}. {item['title']}\n   - {item['link']}"

        send_long_message(chat_id, report)
        return "ok"
    except Exception:
        logger.exception("뉴스 자동 보고 실패")
        send_message(chat_id, "뉴스 자동 보고 처리 중 오류가 발생했습니다.")
        return "error"
