import copy
from difflib import SequenceMatcher
import json
from typing import Any, Dict, Optional, Tuple

from app import config
from app.db_engine import InvestmentDB
from app.formatters.query import build_search_answer, humanize_query_conditions, summarize_query_json
from app.logger import get_logger
from app.parsers.query import build_fixed_query_advice, parse_query
from app.parsers.query_suggestions import suggest_queries
from app.services.response_writer import write_natural_answer
from app.services.telegram import answer_callback_query, edit_message_text, send_message, send_message_with_keyboard
from app.state import dialog_memory, query_suggestions, question_limit
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
        retrieved = db.search(query_json)
        if (retrieved.get("summary") or {}).get("count_projects_total", 0) == 0:
            _send_query_suggestions(db, chat_id, question, query_json)
            return
        effective_query_json, retry_info = query_json, None       
        interpretation = _build_interpretation(effective_query_json, retry_info)
        factual_answer = build_search_answer(retrieved, interpretation)
        answer = write_natural_answer(
            answer_kind="portfolio_query",
            user_question=question,
            interpretation=interpretation,
            factual_answer=factual_answer,
        )
        send_message(chat_id, answer)

        _store_query_context(chat_id, effective_query_json, interpretation, retrieved)

    except Exception:
        logger.exception("query command failed")
        send_message(chat_id, "조회 처리 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.")


def _send_query_suggestions(db, chat_id, question, query_json):
    available_sectors = _available_sectors(db, query_json)
    candidates = suggest_queries(question, query_json, available_sectors)
    # 결과가 존재하는 대안만 노출한다. 누른 뒤 또 0건이 나오는 버튼은 만들지 않는다.
    candidates = [
        item for item in candidates
        if (db.search(item["query_json"]).get("summary") or {}).get("count_projects_total", 0) > 0
    ][:3]
    if not candidates:
        candidates = [
            item for item in _build_fallback_suggestions(query_json, available_sectors)
            if (db.search(item["query_json"]).get("summary") or {}).get("count_projects_total", 0) > 0
        ][:3]
    if not candidates:
        send_message(chat_id, "조건에 맞는 조회 결과가 없습니다. 조건을 조금 넓혀 다시 질문해 주세요.")
        return
    token = query_suggestions.create(chat_id, candidates)
    buttons = [[{"text": item["label"], "callback_data": f"qs:{token}:{idx}"}]
               for idx, item in enumerate(candidates)]
    buttons.append([{"text": "취소", "callback_data": f"qs:{token}:cancel"}])
    send_message_with_keyboard(
        chat_id,
        "조건에 맞는 조회 결과가 없습니다. 아래 대안 중 하나를 선택하거나 취소해 주세요.",
        {"inline_keyboard": buttons},
    )


def _available_sectors(db: InvestmentDB, query_json: Dict[str, Any]):
    return db.available_sectors(query_json)


_SECTOR_PARENT_HINTS = {
    # 세부 표현 -> DB에서 흔히 쓰는 상위 섹터 후보. 한글/영문 질문 모두 처리한다.
    "transportation": (
        "rail", "railway", "철도", "airport", "공항", "road", "도로", "highway", "고속도로",
        "port", "항만", "shipping", "해운", "mobility", "모빌리티", "transport", "교통",
    ),
    "energy": (
        "solar", "태양광", "wind", "풍력", "offshorewind", "해상풍력", "power", "발전",
        "battery", "배터리", "ess", "renewable", "신재생", "hydrogen", "수소", "lng", "가스",
    ),
    "environmental": (
        "waste", "폐기물", "recycling", "재활용", "water", "수처리", "wastewater", "하수",
        "environment", "환경", "circular", "순환경제",
    ),
    "social": (
        "school", "학교", "education", "교육", "public", "공공", "community", "커뮤니티",
    ),
    "digital": (
        "fiber", "광케이블", "broadband", "통신망", "telecom", "통신", "tower", "통신탑",
    ),
    "data center": ("datacenter", "데이터센터", "데이터 센터", "cloud", "클라우드"),
    "logistics": ("warehouse", "창고", "distribution", "물류", "fulfillment", "풀필먼트"),
    "healthcare": ("hospital", "병원", "clinic", "의료", "seniorhousing", "시니어하우징"),
    "residential": ("housing", "주택", "apartment", "아파트", "multifamily", "임대주택"),
    "office": ("오피스", "사무실"),
    "retail": ("리테일", "상업시설", "shoppingmall", "쇼핑몰"),
}


def _compact(value: Any) -> str:
    return "".join(ch for ch in str(value).strip().lower() if ch.isalnum())


def _sector_replacement(sectors, available_sectors):
    available_by_norm = {_compact(value): str(value) for value in available_sectors}
    for sector in sectors:
        normalized = _compact(sector)
        for parent, aliases in _SECTOR_PARENT_HINTS.items():
            parent_norm = _compact(parent)
            alias_norms = {_compact(alias) for alias in aliases}
            if normalized == parent_norm or parent_norm in normalized or normalized in alias_norms:
                if _compact(parent) in available_by_norm:
                    return available_by_norm[_compact(parent)]

        # 오탈자나 Rail Infrastructure 같은 복합 영문 표현도 실제 DB 값과 연결한다.
        ranked = sorted(
            ((SequenceMatcher(None, normalized, key).ratio(), value) for key, value in available_by_norm.items()),
            reverse=True,
        )
        if ranked and ranked[0][0] >= 0.78:
            return ranked[0][1]
    return None


def _build_fallback_suggestions(query_json: Dict[str, Any], available_sectors):
    """Provide useful choices even when the suggestion model is unavailable."""
    filters = query_json.get("filters", {}) or {}
    suggestions = []
    sectors = filters.get("sector") or []
    replacement = _sector_replacement(sectors, available_sectors)
    if replacement:
        broader = copy.deepcopy(query_json)
        broader["filters"]["sector"] = [replacement]
        suggestions.append({
            "label": f"'{', '.join(sectors)}' 대신 '{replacement}'로 찾을까요?"[:35],
            "query_json": broader,
        })
    regions = filters.get("region") or []
    if "KOR" in regions:
        overseas = copy.deepcopy(query_json)
        overseas["filters"]["region"] = ["US", "Europe", "Asia", "Global", "MENA", "Canada"]
        suggestions.append({"label": "국내 대신 해외에서 찾을까요?", "query_json": overseas})

    # 모델이 대안을 반환하지 못해도 특정 질문에 종속되지 않도록 모든 주요 조건을
    # 한 번에 하나씩만 완화한다. 실제 결과가 있는지는 호출부에서 다시 검증한다.
    relax_labels = (
        ("strategy", "전략 조건 없이 찾을까요?"),
        ("manager", "운용사 조건 없이 찾을까요?"),
        ("asset_class", "자산군 조건 없이 찾을까요?"),
        ("investment_type", "투자유형 조건 없이 찾을까요?"),
        ("capital_structure", "자본구조 조건 없이 찾을까요?"),
        ("currency", "통화 조건 없이 찾을까요?"),
    )
    for key, label in relax_labels:       
        if filters.get(key) not in (None, [], {}, ""):
            relaxed = copy.deepcopy(query_json)
            relaxed["filters"].pop(key, None)
            if _has_active_filters(relaxed["filters"]):
                suggestions.append({"label": label, "query_json": relaxed})
    return suggestions[:3]


def handle_query_suggestion_callback(db: InvestmentDB, callback: Dict[str, Any]) -> None:
    callback_id = callback.get("id", "")
    data = str(callback.get("data") or "")
    message = callback.get("message") or {}
    chat_id = (message.get("chat") or {}).get("id")
    message_id = message.get("message_id")
    parts = data.split(":", 2)
    if len(parts) != 3 or parts[0] != "qs":
        return
    entry = query_suggestions.pop(parts[1], chat_id)
    if not entry:
        answer_callback_query(callback_id, "선택 시간이 만료되었습니다.")
        return
    if parts[2] == "cancel":
        answer_callback_query(callback_id, "취소했습니다.")
        edit_message_text(chat_id, message_id, "대안 조회를 취소했습니다.")
        return
    try:
        selected = entry["suggestions"][int(parts[2])]
    except (ValueError, IndexError, KeyError):
        answer_callback_query(callback_id, "유효하지 않은 선택입니다.")
        return
    answer_callback_query(callback_id, "선택한 조건으로 조회합니다.")
    edit_message_text(chat_id, message_id, f"선택: {selected['label']}")
    handle_search_followup(db, chat_id, selected["query_json"])


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
        factual_answer = build_search_answer(retrieved, interpretation)
        answer = write_natural_answer(
            answer_kind="portfolio_query_followup",
            user_question=interpretation,
            interpretation=interpretation,
            factual_answer=factual_answer,
        )
        send_message(chat_id, answer)
        _store_query_context(chat_id, effective_query_json, interpretation, retrieved)
    except Exception:
        logger.exception("query followup failed")
        send_message(chat_id, "후속 조회 처리 중 오류가 발생했습니다.")
