import copy
import json
from typing import Any, Dict, Optional, Tuple

from app import config
from app.db_engine import InvestmentDB
from app.formatters.query import build_search_answer, humanize_query_conditions, summarize_query_json
from app.logger import get_logger
from app.parsers.query import build_fixed_query_advice, parse_query
from app.services.telegram import send_message
from app.state import dialog_memory, question_limit
from app.util import get_sender_display_name

logger = get_logger(__name__)


_RELAX_FILTER_GROUPS = [
    ("capital_structure",),
    ("fund_name_keywords", "asset_name_keywords"),
    ("investment_type", "detail_type"),
    ("manager",),
    ("currency",),
    ("has_lookthrough", "tranche_count_min"),
    ("irr_min", "irr_max", "dpi_min", "dpi_max", "tvpi_min", "tvpi_max", "drawdown_min", "drawdown_max"),
    ("commit_min", "commit_max", "called_min", "called_max", "repaid_min", "repaid_max", "outstanding_min", "outstanding_max", "nav_min", "nav_max", "unfunded_min", "unfunded_max"),
    ("maturity_date_from", "maturity_date_to", "initial_date_from", "initial_date_to"),
    ("vintage_from", "vintage_to", "maturity_year_from", "maturity_year_to"),
    ("region",),
    ("asset_class",),
    ("strategy", "sector"),
]


def _has_active_filters(filters: Dict[str, Any]) -> bool:
    """Return whether a query still has a meaningful portfolio constraint."""
    return any(value not in (None, [], {}, "") for value in filters.values())


def _search_with_auto_relaxation(db: InvestmentDB, query_json: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any], Optional[Dict[str, Any]]]:
    retrieved = db.search(query_json)
    if (retrieved.get("summary") or {}).get("count_projects_total", 0) > 0:
        return retrieved, query_json, None

    filters = query_json.get("filters", {}) or {}
    if not filters or filters.get("project_id"):
        return retrieved, query_json, None

    relaxed_query = copy.deepcopy(query_json)
    relaxed_filters = relaxed_query.setdefault("filters", {})
    for group in _RELAX_FILTER_GROUPS:
        if not any(relaxed_filters.get(key) not in (None, [], {}, "") for key in group):
            continue

        # A retry must retain at least one of the user's original constraints.
        # Otherwise a no-result search can silently become a whole-portfolio
        # lookup, which is neither a useful relaxation nor the requested query.
        candidate_filters = copy.deepcopy(relaxed_filters)
        for key in group:
            candidate_filters.pop(key, None)
        if not _has_active_filters(candidate_filters):
            continue       
        
        for key in group:
            relaxed_filters.pop(key, None)
        relaxed_retrieved = db.search(relaxed_query)
        if (relaxed_retrieved.get("summary") or {}).get("count_projects_total", 0) > 0:
            retry_info = {
                "original_query_json": query_json,
                "relaxed_query_json": copy.deepcopy(relaxed_query),
            }
            return relaxed_retrieved, retry_info["relaxed_query_json"], retry_info

    return retrieved, query_json, None


def _build_interpretation(query_json: Dict[str, Any], retry_info: Optional[Dict[str, Any]]) -> str:
    if not retry_info:
        return summarize_query_json(query_json)
    original = humanize_query_conditions(retry_info["original_query_json"])
    relaxed = humanize_query_conditions(retry_info["relaxed_query_json"])
    return f"{original}으로 조회하였으나 결과가 없어서 {relaxed}으로 수정 조회했습니다."


def _check_limit_or_reply(chat_id: int, ctx: Dict[str, Any]) -> bool:
    sender = ctx.get("sender_user_id")
    allowed, _ = question_limit.check_and_increment(sender, config.DAILY_QUESTION_LIMIT)
    if not allowed:
        name = get_sender_display_name(ctx)
        send_message(chat_id, f"{name}님은 오늘 조회/분석 한도({config.DAILY_QUESTION_LIMIT}회)를 모두 사용했습니다.")
        return False
    return True


def handle_query_command(db: InvestmentDB, chat_id: int, raw: str, ctx: Dict[str, Any]) -> None:
    question = raw[len("/조회"):].strip()
    if not question:
        send_message(chat_id, "조회할 내용을 입력해 주세요. 예: /조회 미국 부동산 투자 현황")
        return

    if not _check_limit_or_reply(chat_id, ctx):
        return

    try:
        parsed = parse_query(question)
        if parsed.get("mode") == "advice":
            send_message(chat_id, parsed.get("advice_text") or build_fixed_query_advice())
            return

        query_json = parsed.get("query_json")
        if not query_json:
            send_message(chat_id, build_fixed_query_advice())
            return

        logger.info("query_json=%s", json.dumps(query_json, ensure_ascii=False))
        retrieved, effective_query_json, retry_info = _search_with_auto_relaxation(db, query_json)
        interpretation = _build_interpretation(effective_query_json, retry_info)
        answer = build_search_answer(retrieved, interpretation)
        send_message(chat_id, answer)

        _store_query_context(chat_id, effective_query_json, interpretation, retrieved)

    except Exception:
        logger.exception("query command failed")
        send_message(chat_id, "조회 처리 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.")


def _store_query_context(chat_id, query_json, interpretation, retrieved):
    rows = retrieved.get("rows") or []
    extras = {
        "rows": [
            {
                "project_id": r.get("Project_ID"),
                "asset_name": r.get("Asset_Name"),
                "sub_asset_count": int(r.get("Sub_Asset_Count") or 0),
            }
            for r in rows[:20]
        ]
    }
    dialog_memory.set_context(chat_id, "query", query_json, interpretation, extras=extras)


def handle_search_followup(db: InvestmentDB, chat_id: int, query_json: Dict[str, Any]) -> None:
    try:
        retrieved, effective_query_json, retry_info = _search_with_auto_relaxation(db, query_json)
        interpretation = _build_interpretation(effective_query_json, retry_info)
        answer = build_search_answer(retrieved, interpretation)
        send_message(chat_id, answer)
        _store_query_context(chat_id, effective_query_json, interpretation, retrieved)
    except Exception:
        logger.exception("query followup failed")
        send_message(chat_id, "후속 조회 처리 중 오류가 발생했습니다.")
