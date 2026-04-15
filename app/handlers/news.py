from datetime import datetime
from typing import Any, Dict, List

from app import config
from app.db_engine import InvestmentDB
from app.logger import get_logger
from app.parsers.news_summary import summarize_news
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


# =========================================================
# Keyword sources
# =========================================================
def _macro_keywords() -> List[str]:
    return list(config.NEWS_KEYWORDS[:10])


def _manager_keywords(db: InvestmentDB) -> List[str]:
    try:
        return db.top_managers_by_outstanding(config.NEWS_MANAGER_KEYWORD_LIMIT)
    except Exception:
        logger.exception("manager keyword build failed")
        return []


# =========================================================
# Collection
# =========================================================
def _collect_articles(keywords: List[str]) -> List[Dict[str, Any]]:
    all_items: List[Dict[str, Any]] = []
    seen = set()

    for kw in keywords:
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


def collect_news_for_keywords(db: InvestmentDB) -> List[Dict[str, Any]]:
    """거시 키워드(NEWS_KEYWORDS) 기반 수집 — 09:10/15:30 슬롯."""
    return _collect_articles(_macro_keywords())


def collect_manager_news(db: InvestmentDB) -> List[Dict[str, Any]]:
    """포트폴리오 운용사(top Managers by Outstanding) 기반 수집 — 09:00 슬롯."""
    keywords = _manager_keywords(db)
    if not keywords:
        logger.warning("manager keyword list empty; skipping manager news report")
        return []
    return _collect_articles(keywords)


# =========================================================
# Slot checks
# =========================================================
def _matches_slot(slot_times: List[str]) -> bool:
    """KST 기준으로 현재 시각이 주어진 시간 목록 중 하나와 ±15분 이내인지."""
    now = datetime.now(KST)
    today = now.strftime("%Y-%m-%d")
    for t in slot_times:
        try:
            base = datetime.strptime(f"{today} {t}", "%Y-%m-%d %H:%M").replace(tzinfo=KST)
        except ValueError:
            continue
        if abs((now - base).total_seconds()) <= 900:
            return True
    return False


# =========================================================
# Reports
# =========================================================
def _send_report(chat_id, header: str, news_items: List[Dict[str, Any]], query: str) -> str:
    try:
        if not news_items:
            send_message(chat_id, f"{header}: 신규 뉴스 없음")
            return "empty"

        summary = summarize_news(query, news_items)

        slot = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
        report = f"{header} ({slot})\n\n{summary}\n\n[수집 기사 {len(news_items)}건]\n"
        for i, item in enumerate(news_items[:10], 1):
            report += f"\n{i}. {item['title']}\n   - {item['link']}"

        send_long_message(chat_id, report)
        return "ok"
    except Exception:
        logger.exception("뉴스 자동 보고 실패 | header=%s", header)
        send_message(chat_id, f"{header} 처리 중 오류가 발생했습니다.")
        return "error"


def run_scheduled_news_report(db: InvestmentDB, chat_id, force: bool = False) -> str:
    """거시 뉴스 자동 보고. NEWS_REPORT_TIMES 슬롯에만 실행."""
    if not config.NEWS_AUTO_REPORT_ENABLED:
        return "disabled"

    if not force and not _matches_slot(config.NEWS_REPORT_TIMES):
        return "skipped"

    news_items = collect_news_for_keywords(db)
    return _send_report(chat_id, "📰 거시 뉴스 자동 보고", news_items, "거시 뉴스")


def run_manager_news_report(db: InvestmentDB, chat_id, force: bool = False) -> str:
    """운용사 뉴스 자동 보고. NEWS_MANAGER_REPORT_TIMES 슬롯에만 실행."""
    if not config.NEWS_AUTO_REPORT_ENABLED:
        return "disabled"

    if not force and not _matches_slot(config.NEWS_MANAGER_REPORT_TIMES):
        return "skipped"

    news_items = collect_manager_news(db)
    return _send_report(chat_id, "🏦 운용사 뉴스 자동 보고", news_items, "포트폴리오 운용사 뉴스")
