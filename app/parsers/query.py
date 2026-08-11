import json
import re
from typing import Any, Dict, List, Optional

from app import config
from app.constants import (
    ASSET_CLASS_ALLOWED,
    ASSET_CLASS_STD_MAP,
    CURRENCY_ALLOWED,
    REGION_ALLOWED,
    SORT_BY_ALLOWED,
    SORT_ORDER_ALLOWED,
)
from app.logger import get_logger
from app.parsers import render_prompt, safe_json_parse
from app.services import gemini
from app.util import get_kst_today_year, normalize_text

logger = get_logger(__name__)

# /조회 BS00001234 / "BS00001234, BS00005678" 같이 ID 만 들어오는 케이스는
# Gemini 거치지 않고 즉시 query_json 으로 변환 — 호출 자체를 0건으로.
_PID_ONLY_PAT = re.compile(
    r"^[\s,]*BS\d{6,10}(?:[\s,]+BS\d{6,10})*[\s,]*$",
    re.IGNORECASE,
)
_PID_FIND_PAT = re.compile(r"BS\d{6,10}", re.IGNORECASE)
_THIS_YEAR_MATURITY_PAT = re.compile(
    r"(올해|금년|이번\s*년(?:도)?|this\s+year).{0,12}만기|"
    r"만기.{0,12}(올해|금년|이번\s*년(?:도)?|this\s+year)",
    re.IGNORECASE,
)
_NEXT_YEAR_MATURITY_PAT = re.compile(
    r"(내년|다음\s*년(?:도)?|next\s+year).{0,12}만기|"
    r"만기.{0,12}(내년|다음\s*년(?:도)?|next\s+year)",
    re.IGNORECASE,
)


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


def _apply_relative_maturity_filters(query_json: Dict[str, Any], user_question: str) -> Dict[str, Any]:
    """Fill maturity year filters for common relative-year Korean expressions.

    The LLM prompt also explains these expressions, but this deterministic
    post-processing keeps queries like "올해 만기" stable even if the model omits
    or misreads the current year.
    """
    text = user_question or ""
    if "만기" not in text:
        return query_json

    filters = query_json.setdefault("filters", {})
    if any(
        filters.get(k) not in (None, "", [], {})
        for k in ("maturity_year_from", "maturity_year_to", "maturity_date_from", "maturity_date_to")
    ):
        return query_json

    year: Optional[int] = None
    if _THIS_YEAR_MATURITY_PAT.search(text):
        year = get_kst_today_year()
    elif _NEXT_YEAR_MATURITY_PAT.search(text):
        year = get_kst_today_year() + 1

    if year is not None:
        filters["maturity_year_from"] = year
        filters["maturity_year_to"] = year
    return query_json


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


def build_gemini_failure_advice() -> str:
    """Gemini 호출 자체가 실패(None/parse 실패)했을 때 — 정상 advice 와 분리해서
    '서비스 일시 장애' 임을 명시. 사용자가 본인 질문 모호 vs 시스템 장애를 구분 가능."""
    return (
        "⚠ 자연어 해석 일시 불가\n"
        "Gemini 응답이 비어있거나 파싱에 실패했습니다.\n"
        "잠시 후 다시 시도하거나, 펀드 ID(BS00001234)를 직접 입력해 주세요."
    )


_DATE_FULL_PAT = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATE_MONTH_PAT = re.compile(r"^\d{4}-\d{2}$")


def _norm_date_filter(val: Any, mode: str) -> Optional[str]:
    """'YYYY-MM-DD' 그대로, 'YYYY-MM' 은 mode='from'→1일, mode='to'→말일로 expand."""
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    if _DATE_FULL_PAT.match(s):
        return s
    if _DATE_MONTH_PAT.match(s):
        from calendar import monthrange
        try:
            y, m = (int(p) for p in s.split("-"))
            if mode == "from":
                return f"{s}-01"
            last = monthrange(y, m)[1]
            return f"{s}-{last:02d}"
        except (TypeError, ValueError):
            return None
    return None


def _norm_str_list(val: Any) -> List[str]:
    if not isinstance(val, list):
        return []
    return [str(x).strip() for x in val if str(x).strip()]


_HANGUL_TOKEN_PAT = re.compile(r"[가-힣A-Za-z0-9&.\-]+")
_HANGUL_PARTICLE_SUFFIXES = (
    "으로", "에서", "에게", "까지", "부터", "처럼", "보다", "이며", "이고",
    "은", "는", "이", "가", "을", "를", "과", "와", "의", "로", "중",
)
_MANAGER_VERB_SUFFIXES = (
    "하는", "하고", "하며", "하면", "해서", "했다", "했던", "하는지", "하던",
    "되는", "되고", "되며", "되면", "됐다", "되던",
)
_MANAGER_KOREAN_STOPWORDS = {
    normalize_text(x)
    for x in [
        "조회", "검색", "분석", "전체", "포트폴리오", "자산", "자산군", "펀드", "운용사",
        "투자", "약정", "잔액", "콜", "인출", "회수", "수익률", "평균", "비중",
        "높은", "낮은", "큰", "작은", "상위", "하위", "순", "기준", "목록",
        "미국", "유럽", "아시아", "글로벌", "국내", "해외", "한국", "캐나다", "중동",
        "부동산", "인프라", "사모", "대출", "사모대출", "벤처", "민자", "비민자",
        "전략", "섹터", "코어", "밸류애드", "오퍼튜니스틱", "시니어", "메자닌",
        "운용", "운용하", "운용하는", "운용한", "운용중", "관리", "보유",
        "만기", "최초", "룩쓰루", "가능", "나온", "중인", "이상", "이하", "초과", "미만",
        "통해", "통해서", "투자된", "투자한", "투자하는", "올해", "금년", "내년",
        "이번", "다음", "년도", "뭐야", "무엇", "무엇이", "어떤", "중에", "퍼드",
    ]
}


def _strip_hangul_particle(token: str) -> str:
    for suffix in _HANGUL_PARTICLE_SUFFIXES:
        if token.endswith(suffix) and len(token) > len(suffix) + 1:
            return token[: -len(suffix)]
    return token


def _is_manager_noise_token(token: str) -> bool:
    norm = normalize_text(token)
    if not norm or norm in _MANAGER_KOREAN_STOPWORDS:
        return True
    # Korean helper verbs like "운용하는" are usually query grammar, not manager
    # names.  Check both before and after particle stripping so "운용하는" does
    # not become the bogus manager keyword "운용하" after removing "는".
    if token.endswith(_MANAGER_VERB_SUFFIXES):
        return True
    return False


def _extract_korean_manager_candidates(user_question: str) -> List[str]:
    """Return likely Korean manager-name tokens from the original user text.

    LLMs often translate a Korean 운용사명 into an English standard name.  For
    search we keep those English names, but also OR-search the original Korean
    surface form so Korean manager values stored in the portfolio are not lost.
    This runs only when the LLM already identified a manager filter; the stopword
    list prevents generic query terms such as "부동산"/"펀드" from becoming
    manager keywords.
    """
    candidates: List[str] = []
    seen = set()
    for match in _HANGUL_TOKEN_PAT.finditer(user_question or ""):
        raw_token = match.group(0).strip("-/.,:;()[]{}<>\"'`")
        if _is_manager_noise_token(raw_token):
            continue
        token = _strip_hangul_particle(raw_token)
        if not token or not re.search(r"[가-힣]", token):
            continue
        if _is_manager_noise_token(token):
            continue
        if len(token) < 2 or any(ch.isdigit() for ch in token):
            continue
        norm = normalize_text(token)
        if norm in _MANAGER_KOREAN_STOPWORDS:
            continue
        if norm and norm not in seen:
            seen.add(norm)
            candidates.append(token)
    return candidates


def preserve_original_korean_managers(filters: Dict[str, Any], user_question: str) -> Dict[str, Any]:
    """Append Korean manager surface forms from the user text to manager filters."""
    managers = _norm_str_list(filters.get("manager"))
    if not managers:
        return filters

    seen = {normalize_text(x) for x in managers}
    for candidate in _extract_korean_manager_candidates(user_question):
        norm = normalize_text(candidate)
        if norm not in seen:
            managers.append(candidate)
            seen.add(norm)

    filters["manager"] = managers[:10]
    return filters



def prefer_strategy_sector_over_capital_structure(filters: Dict[str, Any], user_question: str) -> Dict[str, Any]:
    """Avoid weak capital-structure filters unless explicitly requested.

    The source capital-structure field mostly contains broad legal forms such as
    loan/beneficiary-certificate, so user-facing investment terms are better
    searched through Strategy/Sector.
    """
    if not filters or "capital_structure" not in filters:
        return filters
    if "자본구조" in (user_question or ""):
        return filters

    strategy_aliases = {
        "mezzanine": "mezzanine",
        "메자닌": "mezzanine",
        "equity": "equity",
        "지분": "equity",
        "senior": "senior",
        "시니어": "senior",
    }
    strategies = _norm_str_list(filters.get("strategy"))
    seen = {normalize_text(x) for x in strategies}

    for raw in _norm_str_list(filters.get("capital_structure")):
        mapped = strategy_aliases.get(normalize_text(raw)) or strategy_aliases.get(raw.strip().lower())
        if mapped and normalize_text(mapped) not in seen:
            strategies.append(mapped)
            seen.add(normalize_text(mapped))

    if strategies:
        filters["strategy"] = strategies[:10]
    filters.pop("capital_structure", None)
    return filters


def clean_filters_with_gemini(filters: Dict[str, Any], user_question: str) -> Dict[str, Any]:
    """Use Gemini to remove conversational noise from all parsed filters.

    The first parser pass may accidentally place particles, question words, or
    helper verbs into any string-list filter, not only manager. This focused
    cleanup pass reviews the entire filter dict against the original question so
    we do not need to enumerate every possible Korean phrasing in code.
    """
    if not filters or not gemini.is_available():
        return filters

    prompt = render_prompt(
        "filter_cleaner.txt",
        user_question=user_question,
        filters=filters,
    )
    raw = gemini.generate_json(prompt, max_output_tokens=500, temperature=0.0)
    if not raw:
        logger.warning("filter cleanup: Gemini returned empty response")
        return filters

    try:
        data = safe_json_parse(raw)
    except Exception:
        logger.exception("filter cleanup: JSON parse failed")
        return filters

    cleaned_filters = data.get("filters")
    if not isinstance(cleaned_filters, dict):
        return filters

    return _normalize_filter_dict(cleaned_filters)


def _normalize_filter_dict(filters: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    asset_classes: List[str] = []
    for raw in _norm_str_list(filters.get("asset_class")):
        std = ASSET_CLASS_STD_MAP.get(normalize_text(raw), raw)
        if std in ASSET_CLASS_ALLOWED and std not in asset_classes:
            asset_classes.append(std)
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
        if key == "manager":
            vals = [x for x in vals if not _is_manager_noise_token(x)]
        if vals:
            out[key] = vals[:10]

    for key in ["vintage_from", "vintage_to", "maturity_year_from", "maturity_year_to", "tranche_count_min"]:
        val = filters.get(key)
        if val is not None:
            try:
                out[key] = int(val)
            except (TypeError, ValueError):
                pass

    for key in ("maturity_date_from", "initial_date_from"):
        norm = _norm_date_filter(filters.get(key), "from")
        if norm:
            out[key] = norm
    for key in ("maturity_date_to", "initial_date_to"):
        norm = _norm_date_filter(filters.get(key), "to")
        if norm:
            out[key] = norm

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
        logger.warning("query parse: Gemini unavailable (no API key or SDK missing)")
        return {"mode": "advice", "query_json": None, "advice_text": build_gemini_failure_advice()}

    prompt = render_prompt(
        "query_parser.txt",
        user_question=user_question,
        current_year=get_kst_today_year(),
    )
    previous = ""
    last_advice = ""
    for attempt in range(3):
        attempt_prompt = prompt
        if attempt:
            attempt_prompt += (
                "\n\n[재검토 요청]\n"
                "이전 응답이 비었거나, JSON 문법/스키마 검증에 실패했거나, 실행 가능한 조건을 놓쳤다. "
                "원문에 명시된 조건만 사용하고 임의 조건은 만들지 말 것. 복합 문장을 조건별로 다시 "
                "분해하여 가능한 조회 조건, 정렬, 개수를 최대한 보존한 뒤 위 스키마의 JSON 객체만 출력하라. "
                "정말 조회로 실행할 조건이 전혀 없을 때만 advice를 사용하라.\n"
                f"이전 응답: {previous or '(응답 없음)'}"
            )
        raw = gemini.generate_json(attempt_prompt, max_output_tokens=1000, temperature=0.0)
        previous = raw or ""
        if not raw:
            logger.warning("query parse: Gemini returned empty response | attempt=%s", attempt + 1)
            continue
        try:
            data = safe_json_parse(raw)
        except Exception:
            logger.exception("query parse: JSON parse failed | attempt=%s", attempt + 1)
            continue
            
        mode = str(data.get("mode", "")).strip().lower()
        if mode == "query":
            normalized = normalize_query_json(
                _apply_relative_maturity_filters(data.get("query_json") or {}, user_question)
            )
            normalized["filters"] = preserve_original_korean_managers(normalized.get("filters", {}), user_question)
            normalized["filters"] = prefer_strategy_sector_over_capital_structure(normalized.get("filters", {}), user_question)
            if is_unprocessable_query(normalized):
                previous = json.dumps(data, ensure_ascii=False)
                logger.info("query parse: unprocessable JSON; requesting repair | attempt=%s", attempt + 1)
                continue
            normalized["filters"] = clean_filters_with_gemini(normalized.get("filters", {}), user_question)
            return {"mode": "query", "query_json": normalized, "advice_text": None}

        if mode == "advice":
            last_advice = str(data.get("advice_text") or "").strip()
            if attempt == 0:
                continue
            return {"mode": "advice", "query_json": None, "advice_text": last_advice or build_fixed_query_advice()}

    if last_advice:
        return {"mode": "advice", "query_json": None, "advice_text": last_advice}
    return {"mode": "advice", "query_json": None, "advice_text": build_gemini_failure_advice()}
