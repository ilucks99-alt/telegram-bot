import html as _html
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
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


def handle_morning_briefing_command(db: InvestmentDB, chat_id) -> None:
    """수동 호출용 아침 시장+포트폴리오 통합 브리핑."""
    import threading

    send_message(chat_id, "🌅 시장 + 포트폴리오 통합 브리핑 수집 중...")

    def _worker():
        try:
            run_morning_briefing_report(db, chat_id, force=True)
        except Exception:
            logger.exception("morning briefing command worker failed")
            try:
                send_message(chat_id, "통합 브리핑 처리 중 오류가 발생했습니다.")
            except Exception:
                pass

    threading.Thread(target=_worker, daemon=True).start()


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


def handle_macro_news_command(db: InvestmentDB, chat_id) -> None:
    """/매크로뉴스 — 거시 뉴스 + 매크로 지표 수동 호출 (스케줄 슬롯 무시)."""
    import threading

    send_message(chat_id, "📰 거시 뉴스 + 매크로 지표 수집 중...")

    def _worker():
        try:
            run_scheduled_news_report(db, chat_id, force=True)
        except Exception:
            logger.exception("macro news command worker failed")
            try:
                send_message(chat_id, "매크로 뉴스 처리 중 오류가 발생했습니다.")
            except Exception:
                pass

    threading.Thread(target=_worker, daemon=True).start()


def handle_alternative_news_command(db: InvestmentDB, chat_id) -> None:
    """`대체투자뉴스` 수동 호출용 딜사이트·더벨 브리핑."""
    import threading

    send_message(chat_id, "🏦 딜사이트·더벨 대체투자 이슈 수집 중...")

    def _worker():
        try:
            run_alternative_news_report(db, chat_id, force=True)
        except Exception:
            logger.exception("alternative news command worker failed")
            try:
                send_message(chat_id, "대체투자 뉴스 처리 중 오류가 발생했습니다.")
            except Exception:
                pass

    threading.Thread(target=_worker, daemon=True).start()


# =========================================================
# Keyword sources
# =========================================================
def _macro_keywords() -> List[str]:
    return list(config.NEWS_KEYWORDS)


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

    # GP — 사용자 지정 리스트 우선, 미설정 시 잔액 상위 자동.
    try:
        if config.NEWS_GP_KEYWORDS:
            _add("gp", list(config.NEWS_GP_KEYWORDS))
        else:
            _add("gp", db.top_managers_by_outstanding(config.NEWS_GP_OVERSEAS_LIMIT, overseas_only=True))
            _add("gp", db.top_managers_by_outstanding(config.NEWS_GP_DOMESTIC_LIMIT, domestic_only=True))
    except Exception:
        logger.exception("portfolio news: GP keyword build failed")

    # LookThrough — 부모 펀드 자산군 화이트리스트 (기본 PE/VC).
    try:
        _add("lookthrough", db.top_counterparties_by_book(
            config.NEWS_LOOKTHROUGH_LIMIT,
            parent_asset_classes=config.NEWS_LOOKTHROUGH_ASSET_CLASSES or None,
        ))
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
    items = _collect_articles(_macro_keywords())
    for item in items:
        item["section"] = "market"
    return items


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


_ALTERNATIVE_NEWS_SOURCES = (
    ("딜사이트", "dealsite.co.kr", ("딜사이트", "dealsite")),
    ("더벨", "thebell.co.kr", ("더벨", "the bell", "thebell")),
)


def collect_alternative_news(db: InvestmentDB) -> List[Dict[str, Any]]:
    """딜사이트·더벨의 최근 대체투자 기사만 수집한다.

    원문을 무단 크롤링하지 않고 Google News RSS에 공개된 제목·출처·링크를
    사용한다. 보고서에서는 그 제목에서 확인되는 범위만 요약한다.
    """
    del db  # 다른 collector와 동일한 호출 규약을 유지한다.
    now = datetime.now(KST)
    cutoff = now - timedelta(hours=max(1, config.ALTERNATIVE_NEWS_LOOKBACK_HOURS))
    items: List[Dict[str, Any]] = []
    seen = set()

    for outlet, domain, source_aliases in _ALTERNATIVE_NEWS_SOURCES:
        for topic in config.ALTERNATIVE_NEWS_TOPICS:
            query = f"site:{domain} {topic} when:2d"
            try:
                found = search_google_news_rss(query, limit=config.ALTERNATIVE_NEWS_PER_QUERY_LIMIT)
            except Exception:
                logger.exception("alternative news fetch failed | outlet=%s topic=%s", outlet, topic)
                continue
            for item in found:
                published = item.get("published_at")
                if not published or published.astimezone(KST) < cutoff:
                    continue
                source = str(item.get("source", "")).lower()
                title = str(item.get("title", ""))
                # site: 필터가 무시되는 RSS fallback 결과를 방지한다.
                if domain not in source and not any(alias in source for alias in source_aliases):
                    continue
                key = " ".join(title.lower().split())
                if not key or key in seen:
                    continue
                seen.add(key)
                enriched = dict(item)
                enriched.update(keyword=topic, section=outlet, outlet=outlet)
                items.append(enriched)

    items.sort(key=lambda item: item["published_at"], reverse=True)
    return items[:max(1, config.ALTERNATIVE_NEWS_MAX_ARTICLES)]


# =========================================================
# Reports
# =========================================================
def _format_article_html(item: Dict[str, Any], idx: int) -> str:
    """기사 한 건을 텔레그램 HTML 형식으로 렌더 — title 하이퍼링크 + (source) 이탤릭."""
    title = _html.escape(str(item.get("title", "") or ""))
    link = str(item.get("link", "") or "")
    source = _html.escape(str(item.get("source", "") or ""))
    src_part = f" <i>({source})</i>" if source else ""
    if link:
        return f"{idx}. <a href=\"{link}\">{title}</a>{src_part}"
    return f"{idx}. {title}{src_part}"


def _send_report(
    chat_id,
    header: str,
    news_items: List[Dict[str, Any]],
    query: str,
    macro_prefix: str = "",
) -> str:
    try:
        slot = datetime.now(KST).strftime("%Y-%m-%d %H:%M")

        if not news_items:
            # 뉴스가 없어도 매크로 지표는 쓸모 있으므로 함께 보낸다.
            if macro_prefix:
                send_long_message(chat_id, f"{header} ({slot})\n\n{macro_prefix}\n\n[신규 뉴스 없음]")
            else:
                send_message(chat_id, f"{header}: 신규 뉴스 없음")
            return "empty"

        try:
            summary = summarize_news(query, news_items)
        except Exception:
            logger.exception("summarize_news failed | header=%s", header)
            summary = ""

        # parse_mode=HTML 이므로 동적 텍스트는 모두 escape.
        parts: List[str] = [_html.escape(f"{header} ({slot})", quote=False)]
        if macro_prefix:
            parts.append("")
            parts.append(_html.escape(macro_prefix, quote=False))
        if summary:
            parts.append("")
            parts.append(_html.escape(summary, quote=False))
        parts.append("")
        parts.append(_html.escape(f"[수집 기사 {len(news_items)}건]", quote=False))
        for i, item in enumerate(news_items[:10], 1):
            parts.append(_format_article_html(item, i))

        report = "\n".join(parts)
        send_long_message(chat_id, report, parse_mode="HTML", disable_web_page_preview=True)
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

    # 매크로 지표 스냅샷 — 항상 통합 10개(글로벌+국내) 표시.
    # 실패해도 뉴스 보고는 진행.
    try:
        macro_prefix = market_data.build_macro_briefing(focus="all") or ""
    except Exception:
        logger.exception("macro prefix build failed")
        macro_prefix = ""

    news_items = collect_news_for_keywords(db)
    return _send_report(
        chat_id,
        "📰 거시 뉴스 자동 보고",
        news_items,
        "거시 뉴스",
        macro_prefix=macro_prefix,
    )


def run_alternative_news_report(db: InvestmentDB, chat_id, force: bool = False) -> str:
    """매일 딜사이트·더벨 대체투자 핵심 이슈를 요약·분석해 전송한다."""
    if not config.NEWS_AUTO_REPORT_ENABLED:
        return "disabled"
    if not force and not _matches_slot(config.ALTERNATIVE_NEWS_REPORT_TIMES, "alternative_news"):
        return "skipped"

    news_items = collect_alternative_news(db)
    return _send_report(
        chat_id,
        "🏦 딜사이트·더벨 대체투자 데일리",
        news_items,
        "딜사이트·더벨 대체투자 주요 이슈",
    ) if not news_items else _send_alternative_report(chat_id, news_items)


def _send_alternative_report(chat_id, news_items: List[Dict[str, Any]]) -> str:
    """전문 매체 브리핑을 매체별 링크와 함께 전송한다."""
    try:
        slot = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
        summary = summarize_news(
            "딜사이트·더벨 대체투자 주요 이슈",
            news_items,
            prompt_name="alternative_news_summarizer.txt",
        )
        parts = [_html.escape(f"🏦 대체투자 데일리 ({slot})", quote=False)]
        if summary:
            parts.extend(["", _html.escape(summary, quote=False)])
        for outlet, _domain, _aliases in _ALTERNATIVE_NEWS_SOURCES:
            outlet_items = [item for item in news_items if item.get("outlet") == outlet]
            if not outlet_items:
                continue
            parts.extend(["", _html.escape(f"— {outlet} ({len(outlet_items)}건) —", quote=False)])
            for idx, item in enumerate(outlet_items[:7], 1):
                parts.append(_format_article_html(item, idx))
        send_long_message(chat_id, "\n".join(parts), parse_mode="HTML", disable_web_page_preview=True)
        return "ok"
    except Exception:
        logger.exception("alternative news report failed")
        send_message(chat_id, "대체투자 뉴스 보고 처리 중 오류가 발생했습니다.")
        return "error"


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
            summary = summarize_news(
                "포트폴리오 (GP + LookThrough) 뉴스",
                news_items,
                prompt_name="portfolio_news_summarizer.txt",
            )
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
                out.append(_format_article_html(item, i))
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


def _representative_portfolio_items(
    news_items: List[Dict[str, Any]],
    limit: int,
) -> List[Dict[str, Any]]:
    """Keep GP and LookThrough coverage balanced within the morning prompt budget."""
    if limit <= 0:
        return []
    gp_items = [item for item in news_items if item.get("section") == "gp"]
    lt_items = [item for item in news_items if item.get("section") == "lookthrough"]
    result: List[Dict[str, Any]] = []
    for slot in range(max(len(gp_items), len(lt_items))):
        for items in (gp_items, lt_items):
            if slot < len(items):
                result.append(items[slot])
                if len(result) >= limit:
                    return result
    return result


def _send_morning_briefing(
    chat_id,
    macro_prefix: str,
    market_items: List[Dict[str, Any]],
    portfolio_items: List[Dict[str, Any]],
) -> str:
    """Send one concise morning briefing covering both markets and the portfolio."""
    try:
        slot = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
        all_items = market_items + portfolio_items
        try:
            summary = summarize_news(
                "아침 시장 및 포트폴리오 브리핑",
                all_items,
                prompt_name="morning_news_summarizer.txt",
            ) if all_items else "[뉴스 요약]\n- 신규 주요 뉴스 없음"
        except Exception:
            logger.exception("morning briefing summarize failed")
            summary = ""

        parts = [_html.escape(f"🌅 아침 시장 & 포트폴리오 브리핑 ({slot})", quote=False)]
        if macro_prefix:
            parts.extend(["", _html.escape(macro_prefix, quote=False)])
        if summary:
            parts.extend(["", _html.escape(summary, quote=False)])

        def _links(label: str, items: List[Dict[str, Any]]) -> None:
            if not items:
                return
            parts.extend(["", _html.escape(label, quote=False)])
            for i, item in enumerate(items[:5], 1):
                parts.append(_format_article_html(item, i))

        _links("— 시장 대표 기사 —", market_items)
        _links("— GP 대표 기사 —", [a for a in portfolio_items if a.get("section") == "gp"])
        _links(
            "— LookThrough 대표 기사 —",
            [a for a in portfolio_items if a.get("section") == "lookthrough"],
        )
        send_long_message(
            chat_id,
            "\n".join(parts),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        return "ok"
    except Exception:
        logger.exception("morning briefing send failed")
        send_message(chat_id, "아침 시장 & 포트폴리오 브리핑 처리 중 오류가 발생했습니다.")
        return "error"


def run_morning_briefing_report(db: InvestmentDB, chat_id, force: bool = False) -> str:
    """Morning scheduled report: market context plus GP/LookThrough news in one report."""
    if not config.NEWS_AUTO_REPORT_ENABLED:
        return "disabled"
    if not force and not _matches_slot(config.NEWS_MORNING_BRIEFING_TIMES, "morning_briefing"):
        return "skipped"

    try:
        macro_prefix = market_data.build_macro_briefing(focus="all") or ""
    except Exception:
        logger.exception("morning macro prefix build failed")
        macro_prefix = ""

    market_items = collect_news_for_keywords(db)[:config.NEWS_MORNING_MARKET_ARTICLE_LIMIT]
    portfolio_items = _representative_portfolio_items(
        collect_portfolio_news(db),
        config.NEWS_MORNING_PORTFOLIO_ARTICLE_LIMIT,
    )
    return _send_morning_briefing(chat_id, macro_prefix, market_items, portfolio_items)


def run_portfolio_news_report(db: InvestmentDB, chat_id, force: bool = False) -> str:
    """On-demand GP/LookThrough report; scheduled delivery is part of the morning briefing."""
    if not config.NEWS_AUTO_REPORT_ENABLED:
        return "disabled"
    if not force:
        return "skipped"

    news_items = collect_portfolio_news(db)
    return _send_portfolio_report(chat_id, news_items)
