from typing import Any, Dict

from app.constants import (
    ANALYSIS_METRIC_ALLOWED,
    ANALYSIS_TYPE_ALLOWED,
    GROUPBY_ALLOWED,
    SORT_ORDER_ALLOWED,
)
from app.logger import get_logger
from app.parsers import render_prompt, safe_json_parse
from app.parsers.query import _normalize_filter_dict
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
        "- /분석 미국 부동산 전략별 평균 IRR"
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
        groupby = [g for g in groupby if g in GROUPBY_ALLOWED][:3]
    else:
        groupby = [groupby] if groupby in GROUPBY_ALLOWED else []
    out["groupby"] = groupby

    metrics = analysis_json.get("metrics") or []
    if isinstance(metrics, list):
        metrics = [m for m in metrics if m in ANALYSIS_METRIC_ALLOWED][:3]
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
        return {"mode": "advice", "analysis_json": None, "advice_text": build_fixed_analysis_advice()}

    prompt = render_prompt("analysis_parser.txt", user_question=user_question)
    raw = gemini.generate_json(prompt, max_output_tokens=700, temperature=0.1)
    if not raw:
        return {"mode": "advice", "analysis_json": None, "advice_text": build_fixed_analysis_advice()}

    try:
        data = safe_json_parse(raw)
    except Exception:
        logger.exception("analysis JSON parse failed")
        return {"mode": "advice", "analysis_json": None, "advice_text": build_fixed_analysis_advice()}

    mode = str(data.get("mode", "")).strip().lower()
    if mode == "analysis":
        normalized = normalize_analysis_json(data.get("analysis_json") or {})
        if is_unprocessable_analysis(normalized):
            return {"mode": "advice", "analysis_json": None, "advice_text": build_fixed_analysis_advice()}
        return {"mode": "analysis", "analysis_json": normalized, "advice_text": None}

    advice = str(data.get("advice_text") or "").strip() or build_fixed_analysis_advice()
    return {"mode": "advice", "analysis_json": None, "advice_text": advice}
