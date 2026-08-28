import json
from typing import Any, Dict

from app.constants import (
    ANALYSIS_METRIC_ALLOWED,
    ANALYSIS_TYPE_ALLOWED,
    GROUPBY_ALLOWED,
    MAX_ANALYSIS_GROUPBYS,
    MAX_ANALYSIS_METRICS,
    SORT_ORDER_ALLOWED,
)
from app.logger import get_logger
from app.parsers import render_prompt, safe_json_parse
from app.parsers.query import (
    _normalize_filter_dict,
    build_gemini_failure_advice,
    clean_filters_with_gemini,
    preserve_original_korean_managers,
)
from app.services import gemini

logger = get_logger(__name__)


def build_fixed_analysis_advice() -> str:
    return (
        "[안내]\n"
        "이 질문은 바로 분석형으로 처리하기 어렵습니다.\n"
        "비중, 평균 수익률, 자산군별/전략별/지역별 분석처럼 계산 기준이 드러나도록 다시 질문해 주세요.\n\n"
        "[예시 분석]\n"
        "- /분석 전체 포트폴리오에서 미국 비중\n"
        "- /분석 미국 부동산 투자 중 Core 전략 비중\n"
        "- /분석 자산군별 평균 IRR\n"
        "- /분석 미국 부동산 전략별 평균 IRR\n"
        "- /분석 자산군별 미인출액과 인출률\n"
        "- /분석 PE 빈티지별 DPI와 TVPI\n"
        "- /분석 약정통화별 투자잔액"
    )


def normalize_analysis_json(analysis_json: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "analysis_type": "share",
        "base_filters": {},
        "target_filters": {},
        "metric": "commitment",
        "groupby": [],
        "metrics": ["commitment"],
        "sort_by": "commitment",
        "top_n": 50,
        "sort_order": "desc",
    }
    if not isinstance(analysis_json, dict):
        return out

    atype = str(analysis_json.get("analysis_type", "share")).strip()
    if atype in ANALYSIS_TYPE_ALLOWED:
        out["analysis_type"] = atype

    out["base_filters"] = _normalize_filter_dict(analysis_json.get("base_filters", {}) or {})
    out["target_filters"] = _normalize_filter_dict(analysis_json.get("target_filters", {}) or {})

    metric = analysis_json.get("metric")
    if metric in ANALYSIS_METRIC_ALLOWED:
        out["metric"] = metric

    groupby = analysis_json.get("groupby") or []
    if isinstance(groupby, list):
        groupby = list(dict.fromkeys(g for g in groupby if g in GROUPBY_ALLOWED))[
            :MAX_ANALYSIS_GROUPBYS
        ]
    else:
        groupby = [groupby] if groupby in GROUPBY_ALLOWED else []
    out["groupby"] = groupby

    metrics = analysis_json.get("metrics") or []
    if isinstance(metrics, list):
        metrics = list(dict.fromkeys(m for m in metrics if m in ANALYSIS_METRIC_ALLOWED))[
            :MAX_ANALYSIS_METRICS
        ]
    else:
        metrics = [metrics] if metrics in ANALYSIS_METRIC_ALLOWED else ["commitment"]
    out["metrics"] = metrics or ["commitment"]

    sort_by = analysis_json.get("sort_by")
    out["sort_by"] = sort_by if sort_by in out["metrics"] else out["metrics"][0]

    try:
        out["top_n"] = max(1, min(int(analysis_json.get("top_n", 50)), 100))
    except (TypeError, ValueError):
        out["top_n"] = 50

    sort_order = str(analysis_json.get("sort_order", "desc")).lower()
    if sort_order in SORT_ORDER_ALLOWED:
        out["sort_order"] = sort_order

    if out["analysis_type"] == "share":
        out["groupby"] = []
        out["metrics"] = []
        out["sort_by"] = ""
        out["top_n"] = 50
        out["sort_order"] = "desc"

    return out


def is_unprocessable_analysis(analysis_json: Dict[str, Any]) -> bool:
    atype = analysis_json.get("analysis_type")
    if atype == "share":
        tf = analysis_json.get("target_filters", {}) or {}
        return not any(v not in (None, [], {}, "") for v in tf.values())
    if atype == "grouped_metric":
        return not (analysis_json.get("groupby") and analysis_json.get("metrics"))
    return True


def parse_analysis(user_question: str) -> Dict[str, Any]:
    if not gemini.is_available():
        logger.warning("analysis parse: Gemini unavailable (no API key or SDK missing)")
        return {"mode": "advice", "analysis_json": None, "advice_text": build_gemini_failure_advice()}

    prompt = render_prompt("analysis_parser.txt", user_question=user_question)
    previous = ""
    last_advice = ""
    for attempt in range(3):
        attempt_prompt = prompt
        if attempt:
            attempt_prompt += (
                "\n\n[재검토 요청]\n"
                "이전 응답이 비었거나 JSON/분석 스키마 검증에 실패했다. 원문을 모집단, 비교 대상, "
                "그룹 기준, 계산 지표, 정렬 조건으로 나누어 다시 해석하라. 원문에 없는 조건은 만들지 "
                "말고, 실행 가능한 분석이 하나라도 있으면 analysis로 작성하라. 위 스키마의 JSON 객체만 "
                f"출력하라.\n이전 응답: {previous or '(응답 없음)'}"
            )
        raw = gemini.generate_json(attempt_prompt, max_output_tokens=1000, temperature=0.0)
        previous = raw or ""
        if not raw:
            logger.warning("analysis parse: Gemini returned empty response | attempt=%s", attempt + 1)
            continue
        try:
            data = safe_json_parse(raw)
        except Exception:
            logger.exception("analysis parse: JSON parse failed | attempt=%s", attempt + 1)
            continue

        mode = str(data.get("mode", "")).strip().lower()
        if mode == "analysis":
            normalized = normalize_analysis_json(data.get("analysis_json") or {})
            if is_unprocessable_analysis(normalized):
                previous = json.dumps(data, ensure_ascii=False)
                logger.info("analysis parse: unprocessable JSON; requesting repair | attempt=%s", attempt + 1)
                continue
            has_base_manager = bool((normalized.get("base_filters") or {}).get("manager"))
            has_target_manager = bool((normalized.get("target_filters") or {}).get("manager"))
            if has_base_manager != has_target_manager:
                filter_key = "base_filters" if has_base_manager else "target_filters"
                normalized[filter_key] = preserve_original_korean_managers(
                    normalized.get(filter_key, {}), user_question
                )
            normalized["base_filters"] = clean_filters_with_gemini(normalized.get("base_filters", {}), user_question)
            normalized["target_filters"] = clean_filters_with_gemini(normalized.get("target_filters", {}), user_question)
            return {"mode": "analysis", "analysis_json": normalized, "advice_text": None}

        if mode == "advice":
            last_advice = str(data.get("advice_text") or "").strip()
            if attempt == 0:
                continue
            return {"mode": "advice", "analysis_json": None, "advice_text": last_advice or build_fixed_analysis_advice()}

    if last_advice:
        return {"mode": "advice", "analysis_json": None, "advice_text": last_advice}
    return {"mode": "advice", "analysis_json": None, "advice_text": build_gemini_failure_advice()}
