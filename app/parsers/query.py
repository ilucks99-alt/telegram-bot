import re
from typing import Any, Dict, List, Optional

from app import config
from app.constants import (
    ASSET_CLASS_ALLOWED,
    CURRENCY_ALLOWED,
    REGION_ALLOWED,
    SORT_BY_ALLOWED,
    SORT_ORDER_ALLOWED,
)
from app.logger import get_logger
from app.parsers import render_prompt, safe_json_parse
from app.services import gemini

logger = get_logger(__name__)

# /조회 BS00001234 / "BS00001234, BS00005678" 같이 ID 만 들어오는 케이스는
# Gemini 거치지 않고 즉시 query_json 으로 변환 — 호출 자체를 0건으로.
_PID_ONLY_PAT = re.compile(
    r"^[\s,]*BS\d{6,10}(?:[\s,]+BS\d{6,10})*[\s,]*$",
    re.IGNORECASE,
)
_PID_FIND_PAT = re.compile(r"BS\d{6,10}", re.IGNORECASE)


def _try_pid_only_shortcut(question: str) -> Optional[Dict[str, Any]]:
    s = (question or "").strip()
    if not s or not _PID_ONLY_PAT.match(s):
        return None
    pids = [p.upper() for p in _PID_FIND_PAT.findall(s)]
    if not pids:
        return None
    return {
        "query_type": "summary_with_list",
        "filters": {"project_id": pids},
        "sort": {},
        "output": {
            "include_summary": True,
            "include_list": True,
            "limit": config.DEFAULT_LIMIT,
        },
    }


def build_fixed_query_advice() -> str:
    return (
        "[안내]\n"
        "이 질문은 바로 조회형으로 처리하기 어렵습니다.\n"
        "자산군, 지역, 전략, 운용사, 펀드명, 만기, 수익률, NAV, 콜금액 기준을 포함해 다시 질문해 주세요.\n\n"
        "[예시 조회]\n"
        "- /조회 미국 PD 펀드 중 IRR 높은 상위 5개\n"
        "- /조회 유럽 인프라 펀드 중 27년 이전 만기 건\n"
        "- /조회 블랙스톤 부동산 펀드\n"
        "- /조회 NAV 큰 순 상위 10개"
    )


def _norm_str_list(val: Any) -> List[str]:
    if not isinstance(val, list):
        return []
    return [str(x).strip() for x in val if str(x).strip()]


def _normalize_filter_dict(filters: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    asset_classes = [x for x in _norm_str_list(filters.get("asset_class")) if x in ASSET_CLASS_ALLOWED]
    if asset_classes:
        out["asset_class"] = asset_classes

    regions = [x for x in _norm_str_list(filters.get("region")) if x in REGION_ALLOWED]
    if regions:
        out["region"] = regions

    currencies = [x for x in _norm_str_list(filters.get("currency")) if x.upper() in CURRENCY_ALLOWED]
    if currencies:
        out["currency"] = [x.upper() if x != "Unknown" else x for x in currencies]

    for key in [
        "manager", "strategy", "sector", "project_id",
        "fund_name_keywords", "asset_name_keywords",
        "investment_type", "detail_type", "capital_structure",
    ]:
        vals = _norm_str_list(filters.get(key))
        if vals:
            out[key] = vals[:10]

    for key in ["vintage_from", "vintage_to", "maturity_year_from", "maturity_year_to", "tranche_count_min"]:
        val = filters.get(key)
        if val is not None:
            try:
                out[key] = int(val)
            except (TypeError, ValueError):
                pass

    has_lt = filters.get("has_lookthrough")
    if has_lt is not None:
        if isinstance(has_lt, str):
            out["has_lookthrough"] = has_lt.strip().lower() in ("1", "true", "yes", "y")
        else:
            out["has_lookthrough"] = bool(has_lt)

    for key in [
        "irr_min", "irr_max", "commit_min", "commit_max",
        "called_min", "called_max", "outstanding_min", "outstanding_max",
        "nav_min", "nav_max", "repaid_min", "repaid_max",
        "dpi_min", "dpi_max", "tvpi_min", "tvpi_max",
        "drawdown_min", "drawdown_max", "unfunded_min", "unfunded_max",
    ]:
        val = filters.get(key)
        if val is not None:
            try:
                out[key] = float(val)
            except (TypeError, ValueError):
                pass

    # IRR/Drawdown 은 소수 저장(0.05)인데 LLM 이 종종 5 같은 정수 % 로 내보낸다.
    # |값| >= 1.0 이면 % 단위로 보고 100 으로 나눔. (DPI/TVPI 는 배수라 1.0+ 가 정상이라 변환 X)
    for key in ("irr_min", "irr_max", "drawdown_min", "drawdown_max"):
        if key in out and abs(out[key]) >= 1.0 and key.startswith("irr"):
            out[key] = out[key] / 100.0
        # drawdown 은 0~1 사이가 정상이지만 LLM 이 "80% 인출률" 을 80 으로 보낼 가능성
        if key.startswith("drawdown") and key in out and out[key] > 1.5:
            out[key] = out[key] / 100.0

    return out


def normalize_query_json(query_json: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "query_type": "summary_with_list",
        "filters": {},
        "sort": {},
        "output": {
            "include_summary": True,
            "include_list": True,
            "limit": config.DEFAULT_LIMIT,
        },
    }
    if not isinstance(query_json, dict):
        return out

    out["filters"] = _normalize_filter_dict(query_json.get("filters", {}) or {})

    sort = query_json.get("sort", {}) or {}
    sort_by = str(sort.get("by", "")).strip()
    sort_order = str(sort.get("order", "")).strip().lower()
    if sort_by in SORT_BY_ALLOWED and sort_order in SORT_ORDER_ALLOWED:
        out["sort"] = {"by": sort_by, "order": sort_order}

    output = query_json.get("output", {}) or {}
    try:
        limit = int(output.get("limit", config.DEFAULT_LIMIT))
    except (TypeError, ValueError):
        limit = config.DEFAULT_LIMIT
    out["output"]["limit"] = max(1, min(limit, config.MAX_LIMIT))

    return out


def is_unprocessable_query(query_json: Dict[str, Any]) -> bool:
    filters = query_json.get("filters", {}) or {}
    if any(v not in (None, [], {}, "") for v in filters.values()):
        return False

    sort = query_json.get("sort", {}) or {}
    limit = int(query_json.get("output", {}).get("limit", config.DEFAULT_LIMIT) or config.DEFAULT_LIMIT)

    if sort.get("by") and limit != config.DEFAULT_LIMIT:
        return False
    return True


def parse_query(user_question: str) -> Dict[str, Any]:
    # ID-only 입력은 LLM 호출 없이 즉시 처리 (토큰 절감)
    shortcut = _try_pid_only_shortcut(user_question)
    if shortcut is not None:
        return {"mode": "query", "query_json": normalize_query_json(shortcut), "advice_text": None}

    if not gemini.is_available():
        return {"mode": "advice", "query_json": None, "advice_text": build_fixed_query_advice()}

    prompt = render_prompt("query_parser.txt", user_question=user_question)
    raw = gemini.generate_json(prompt, max_output_tokens=600, temperature=0.1)
    if not raw:
        return {"mode": "advice", "query_json": None, "advice_text": build_fixed_query_advice()}

    try:
        data = safe_json_parse(raw)
    except Exception:
        logger.exception("query JSON parse failed")
        return {"mode": "advice", "query_json": None, "advice_text": build_fixed_query_advice()}

    mode = str(data.get("mode", "")).strip().lower()
    if mode == "query":
        normalized = normalize_query_json(data.get("query_json") or {})
        if is_unprocessable_query(normalized):
            return {"mode": "advice", "query_json": None, "advice_text": build_fixed_query_advice()}
        return {"mode": "query", "query_json": normalized, "advice_text": None}

    advice_text = str(data.get("advice_text") or "").strip() or build_fixed_query_advice()
    return {"mode": "advice", "query_json": None, "advice_text": advice_text}
