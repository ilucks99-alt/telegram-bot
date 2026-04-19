from concurrent.futures import ThreadPoolExecutor, as_completed
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


def handle_manager_news_command(db: InvestmentDB, chat_id) -> None:
    """/운용사뉴스 — 포트폴리오 운용사 기반 뉴스 리포트 수동 호출."""
    import threading

    send_message(chat_id, "🏦 운용사 뉴스 수집 중...")

    def _worker():
        try:
            run_manager_news_report(db, chat_id)
        except Exception:
            logger.exception("manager news command worker failed")
            try:
                send_message(chat_id, "운용사 뉴스 처리 중 오류가 발생했습니다.")
            except Exception:
                pass

    threading.Thread(target=_worker, daemon=True).start()


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
# Collection (parallel RSS fetching)
# =========================================================
def _fetch_for_keyword(kw: str) -> List[Dict[str, Any]]:
    """단일 키워드에 대해 RSS 수집 (스레드풀에서 병렬 실행)."""
    try:
        items = search_google_news_rss(kw, limit=config.NEWS_PER_KEYWORD_LIMIT)
        for item in items:
            item["keyword"] = kw
        return items
    except Exception:
        logger.exception("뉴스 수집 실패 | keyword=%s", kw)
        return []


def _collect_articles(keywords: List[str]) -> List[Dict[str, Any]]:
    all_items: List[Dict[str, Any]] = []
    seen = set()

    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(_fetch_for_keyword, kw): kw for kw in keywords}
        for future in as_completed(futures):
            for item in future.result():
                key = (item["title"].lower(), item.get("source", ""))
                if key in seen:
                    continue
                seen.add(key)
                all_items.append(item)

    all_items.sort(key=lambda x: x["published_at"], reverse=True)
    return all_items[:config.NEWS_REPORT_MAX_ARTICLES]


def collect_news_for_keywords(db: InvestmentDB) -> List[Dict[str, Any]]:
    return _collect_articles(_macro_keywords())


def collect_manager_news(db: InvestmentDB) -> List[Dict[str, Any]]:
    keywords = _manager_keywords(db)
    if not keywords:
        logger.warning("manager keyword list empty; skipping manager news report")
        return []
    return _collect_articles(keywords)


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


def run_scheduled_news_report(db: InvestmentDB, chat_id, **_kw) -> str:
    """거시 뉴스 자동 보고."""
    if not config.NEWS_AUTO_REPORT_ENABLED:
        return "disabled"

    news_items = collect_news_for_keywords(db)
    return _send_report(chat_id, "📰 거시 뉴스 자동 보고", news_items, "거시 뉴스")


def run_manager_news_report(db: InvestmentDB, chat_id, **_kw) -> str:
    """운용사 뉴스 자동 보고."""
    if not config.NEWS_AUTO_REPORT_ENABLED:
        return "disabled"

    news_items = collect_manager_news(db)
    return _send_report(chat_id, "🏦 운용사 뉴스 자동 보고", news_items, "포트폴리오 운용사 뉴스")
