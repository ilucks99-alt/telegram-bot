import html as _html
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Set

from app import config
from app.db_engine import InvestmentDB
from app.logger import get_logger
from app.parsers.news_summary import summarize_news
from app.services import market_data, sheets
from app.services.news_rss import search_google_news_rss
from app.services.telegram import send_long_message, send_message
from app.util import KST

logger = get_logger(__name__)

# In-memory cache layered on top of Sheets (reduces Sheets reads within a session).
# Persistence across Render restarts is provided by the NewsDedup tab in Sheets.
_sent_slots: Set[str] = set()


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


def handle_portfolio_news_command(db: InvestmentDB, chat_id) -> None:
    """/포트폴리오뉴스 — GP(해외+국내) + LookThrough 발행인 통합 뉴스 수동 호출."""
    import threading

    send_message(chat_id, "📊 포트폴리오 뉴스 수집 중...")

    def _worker():
        try:
            run_portfolio_news_report(db, chat_id, force=True)
        except Exception:
            logger.exception("portfolio news command worker failed")
            try:
                send_message(chat_id, "포트폴리오 뉴스 처리 중 오류가 발생했습니다.")
            except Exception:
                pass

    threading.Thread(target=_worker, daemon=True).start()


# =========================================================
# Keyword sources
# =========================================================
def _macro_keywords() -> List[str]:
    return list(config.NEWS_KEYWORDS[:10])


def _portfolio_keyword_sections(db: InvestmentDB) -> Dict[str, List[str]]:
    """포트폴리오 뉴스 키워드를 섹션별로 분리해 반환.

    {"gp": [...], "lookthrough": [...]} — GP 섹션이 LookThrough 섹션 키워드와
    중복되는 드문 케이스(자체 운용 펀드의 자체 발행인 매핑 등)는 GP 우선으로
    잡고 LookThrough 쪽에서 제거한다."""
    seen = set()
    sections: Dict[str, List[str]] = {"gp": [], "lookthrough": []}

    def _add(section: str, items: List[str]) -> None:
        for kw in items:
            if not kw:
                continue
            key = kw.strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            sections[section].append(kw)

    try:
        _add("gp", db.top_managers_by_outstanding(config.NEWS_GP_OVERSEAS_LIMIT, overseas_only=True))
        _add("gp", db.top_managers_by_outstanding(config.NEWS_GP_DOMESTIC_LIMIT, domestic_only=True))
    except Exception:
        logger.exception("portfolio news: GP keyword build failed")
    try:
        _add("lookthrough", db.top_counterparties_by_book(config.NEWS_LOOKTHROUGH_LIMIT))
    except Exception:
        logger.exception("portfolio news: LookThrough counterparty keyword build failed")

    return sections


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
    """
    키워드별 round-robin 으로 보고 후보를 모아 키워드 간 형평성을 유지한다.

    - 키워드별로 따로 dedup + 최신순 정렬한 뒤
    - slot 0..quota-1 순회하며 각 키워드의 N번째 기사를 차례로 픽업
    - 전체 cap(NEWS_REPORT_MAX_ARTICLES) 도달 시 종료
    - 마지막에 published_at 최신순으로 한 번 더 정렬해 보고서 가독성 유지

    이렇게 안 하면 한국어 매체(분 단위 갱신) 키워드가 영문 매체(시간 단위 갱신)
    키워드를 published_at 정렬에서 모두 밀어내 해외 운용사 결과가 0건이 된다.
    """
    per_keyword: Dict[str, List[Dict[str, Any]]] = {}
    seen = set()

    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(_fetch_for_keyword, kw): kw for kw in keywords}
        for future in as_completed(futures):
            kw = futures[future]
            kw_items: List[Dict[str, Any]] = []
            for item in future.result():
                key = (item["title"].lower(), item.get("source", ""))
                if key in seen:
                    continue
                seen.add(key)
                kw_items.append(item)
            kw_items.sort(key=lambda x: x["published_at"], reverse=True)
            per_keyword[kw] = kw_items

    quota = max(1, int(getattr(config, "NEWS_PER_KEYWORD_REPORT_QUOTA", 3)))
    cap = int(config.NEWS_REPORT_MAX_ARTICLES)

    result: List[Dict[str, Any]] = []
    for slot in range(quota):
        for kw in keywords:
            items = per_keyword.get(kw, [])
            if slot < len(items):
                result.append(items[slot])
                if len(result) >= cap:
                    break
        if len(result) >= cap:
            break

    result.sort(key=lambda x: x["published_at"], reverse=True)

    coverage = sum(1 for kw in keywords if per_keyword.get(kw))
    logger.info(
        "뉴스 수집 결과 | 키워드 %d개 (수신 %d개) → 기사 %d건 (cap=%d, quota/kw=%d)",
        len(keywords), coverage, len(result), cap, quota,
    )
    return result


def collect_news_for_keywords(db: InvestmentDB) -> List[Dict[str, Any]]:
    return _collect_articles(_macro_keywords())


def collect_portfolio_news(db: InvestmentDB) -> List[Dict[str, Any]]:
    sections = _portfolio_keyword_sections(db)
    keyword_to_section: Dict[str, str] = {}
    flat: List[str] = []
    for section, kws in sections.items():
        for kw in kws:
            keyword_to_section[kw] = section
            flat.append(kw)
    if not flat:
        logger.warning("portfolio keyword list empty; skipping portfolio news report")
        return []
    items = _collect_articles(flat)
    for it in items:
        it["section"] = keyword_to_section.get(it.get("keyword", ""), "gp")
    return items


# =========================================================
# Reports
# =========================================================
def _send_report(
    chat_id,
    header: str,
    news_items: List[Dict[str, Any]],
    query: str,
    macro_prefix: str = "",
) -> str:
    try:
        slot = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
        prefix = f"{macro_prefix}\n\n" if macro_prefix else ""

        if not news_items:
            # 뉴스가 없어도 매크로 지표는 쓸모 있으므로 함께 보낸다
            if macro_prefix:
                send_long_message(chat_id, f"{header} ({slot})\n\n{macro_prefix}\n\n[신규 뉴스 없음]")
            else:
                send_message(chat_id, f"{header}: 신규 뉴스 없음")
            return "empty"

        summary = summarize_news(query, news_items)

        report = f"{header} ({slot})\n\n{prefix}{summary}\n\n[수집 기사 {len(news_items)}건]\n"
        for i, item in enumerate(news_items[:10], 1):
            report += f"\n{i}. {item['title']}\n   - {item['link']}"

        send_long_message(chat_id, report)
        return "ok"
    except Exception:
        logger.exception("뉴스 자동 보고 실패 | header=%s", header)
        send_message(chat_id, f"{header} 처리 중 오류가 발생했습니다.")
        return "error"


def _matches_slot(slot_times: List[str], slot_name: str) -> bool:
    """KST 기준 현재 시각이 슬롯 ±15분 이내이고 아직 미전송인지 확인.

    In-memory cache + Google Sheets (NewsDedup 탭) 2단 체크로 Render 재시작 후에도 중복 발송을 막는다.
    """
    now = datetime.now(KST)
    today = now.strftime("%Y-%m-%d")
    for t in slot_times:
        slot_key = f"{slot_name}:{today}:{t}"
        if slot_key in _sent_slots:
            continue
        try:
            base = datetime.strptime(f"{today} {t}", "%Y-%m-%d %H:%M").replace(tzinfo=KST)
        except ValueError:
            continue
        if abs((now - base).total_seconds()) > 900:
            continue

        # 시트에 이미 기록됐으면 재발송 방지 (Render 재시작 시 _sent_slots 유실 대비)
        try:
            if sheets.is_news_slot_sent(slot_key):
                _sent_slots.add(slot_key)
                continue
        except Exception:
            logger.exception("news dedup sheet check failed | key=%s", slot_key)
            # 시트 장애 시에는 in-memory 만으로라도 진행

        _sent_slots.add(slot_key)
        try:
            sheets.mark_news_slot_sent(slot_key)
        except Exception:
            logger.exception("news dedup sheet mark failed | key=%s", slot_key)

        # 이전 날짜 in-memory 키 정리 (시트 정리는 일일 1회 기회적으로)
        for k in list(_sent_slots):
            if k.startswith(slot_name) and today not in k:
                _sent_slots.discard(k)
        try:
            sheets.prune_news_dedup([today])
        except Exception:
            logger.exception("news dedup prune failed")

        return True
    return False


def run_scheduled_news_report(db: InvestmentDB, chat_id, force: bool = False) -> str:
    """거시 뉴스 자동 보고. tick에서 호출 — 슬롯+중복 체크 + 매크로 지표 포함."""
    if not config.NEWS_AUTO_REPORT_ENABLED:
        return "disabled"

    if not force and not _matches_slot(config.NEWS_REPORT_TIMES, "macro_news"):
        return "skipped"

    # Yahoo Finance 에서 주요 매크로 지표 스냅샷 (실패해도 뉴스 보고는 진행).
    # 오전 슬롯(KST 12시 전) = 글로벌 / 오후 슬롯 = 국내 중심.
    focus = "domestic" if datetime.now(KST).hour >= 12 else "global"
    try:
        macro_prefix = market_data.build_macro_briefing(focus=focus) or ""
    except Exception:
        logger.exception("macro prefix build failed | focus=%s", focus)
        macro_prefix = ""

    news_items = collect_news_for_keywords(db)
    return _send_report(
        chat_id,
        "📰 거시 뉴스 자동 보고",
        news_items,
        "거시 뉴스",
        macro_prefix=macro_prefix,
    )


def _send_portfolio_report(chat_id, news_items: List[Dict[str, Any]]) -> str:
    """GP/LookThrough 섹션을 분리해서 보고서 출력. 기사 링크는 텔레그램 HTML
    하이퍼링크(<a>)로 감싸 raw URL 노출을 줄인다 (단축 서비스 의존 X)."""
    try:
        slot = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
        header = "📊 포트폴리오 뉴스 자동 보고"

        if not news_items:
            send_message(chat_id, f"{header} ({slot}): 신규 뉴스 없음")
            return "empty"

        try:
            summary = summarize_news("포트폴리오 (GP + LookThrough) 뉴스", news_items)
        except Exception:
            logger.exception("portfolio news summarize failed")
            summary = ""

        gp_items = [a for a in news_items if a.get("section") == "gp"]
        lt_items = [a for a in news_items if a.get("section") == "lookthrough"]

        # parse_mode=HTML 이므로 모든 동적 텍스트는 escape 필수.
        parts: List[str] = []
        parts.append(_html.escape(f"{header} ({slot})", quote=False))
        if summary:
            parts.append("")
            parts.append(_html.escape(summary, quote=False))
        parts.append("")
        parts.append(_html.escape(f"[수집 기사 {len(news_items)}건]", quote=False))

        def _format_section(label: str, items: List[Dict[str, Any]]) -> List[str]:
            if not items:
                return []
            out = ["", _html.escape(f"— {label} ({len(items)}건) —", quote=False)]
            for i, item in enumerate(items[:10], 1):
                title = _html.escape(str(item.get("title", "") or ""))
                link = str(item.get("link", "") or "")
                source = _html.escape(str(item.get("source", "") or ""))
                # title 하이퍼링크 + 소스명을 옆에 (URL 자체는 안 보임)
                src_part = f" <i>({source})</i>" if source else ""
                if link:
                    out.append(f"{i}. <a href=\"{link}\">{title}</a>{src_part}")
                else:
                    out.append(f"{i}. {title}{src_part}")
            return out

        parts.extend(_format_section("🏦 GP / 운용사", gp_items))
        parts.extend(_format_section("🏢 포트폴리오 발행인 (LookThrough)", lt_items))

        report = "\n".join(parts)
        # 링크 미리보기 카드는 첫 링크만 큰 카드로 잡혀 메시지가 더 길어지므로 끔.
        send_long_message(chat_id, report, parse_mode="HTML", disable_web_page_preview=True)
        return "ok"
    except Exception:
        logger.exception("포트폴리오 뉴스 보고 실패")
        send_message(chat_id, "포트폴리오 뉴스 처리 중 오류가 발생했습니다.")
        return "error"


def run_portfolio_news_report(db: InvestmentDB, chat_id, force: bool = False) -> str:
    """포트폴리오 뉴스 자동 보고 — GP(해외+국내) + LookThrough 발행인 통합. tick에서 호출."""
    if not config.NEWS_AUTO_REPORT_ENABLED:
        return "disabled"

    if not force and not _matches_slot(config.NEWS_PORTFOLIO_REPORT_TIMES, "portfolio_news"):
        return "skipped"

    news_items = collect_portfolio_news(db)
    return _send_portfolio_report(chat_id, news_items)
