from typing import Any, Dict, List

from app.formatters.query import _humanize_filter_summary
from app.util import format_amount_uk, format_pct


def summarize_analysis_json(analysis_json: Dict[str, Any]) -> str:
    atype = analysis_json.get("analysis_type")
    metric = analysis_json.get("metric", "commitment")

    metric_label_map = {
        "commitment": "약정 기준",
        "called": "콜금액 기준",
        "outstanding": "투자잔액 기준",
        "nav": "NAV 기준",
        "count": "건수 기준",
        "irr_avg": "단순평균 IRR 기준",
        "irr_weighted_commitment": "약정가중 평균 IRR 기준",
        "irr_weighted_called": "콜금액가중 평균 IRR 기준",
        "irr_weighted_outstanding": "투자잔액가중 평균 IRR 기준",
        "irr_weighted_nav": "NAV가중 평균 IRR 기준",
    }

    if atype == "share":
        base_parts = _humanize_filter_summary(analysis_json.get("base_filters", {}) or {})
        target_parts = _humanize_filter_summary(analysis_json.get("target_filters", {}) or {})
        base_text = "전체 포트폴리오" if not base_parts else f"모집단({', '.join(base_parts)})"
        target_text = "대상조건" if not target_parts else f"대상({', '.join(target_parts)})"
        return f"{base_text} 대비 {target_text} 비중 분석으로 이해했습니다. ({metric_label_map.get(metric, metric)})"

    if atype == "grouped_metric":
        base_parts = _humanize_filter_summary(analysis_json.get("base_filters", {}) or {})
        base_text = "전체 포트폴리오" if not base_parts else ", ".join(base_parts)

        groupby_list = analysis_json.get("groupby") or []
        metrics_list = analysis_json.get("metrics") or []

        groupby_label_map = {
            "asset_class": "자산군", "region": "지역", "strategy": "전략",
            "manager": "운용사", "sector": "섹터", "vintage": "Vintage", "maturity_year": "만기년도",
        }

        groupby_text = "/".join([groupby_label_map.get(g, g) for g in groupby_list]) if groupby_list else "그룹"
        metric_text = "/".join([metric_label_map.get(m, m) for m in metrics_list]) if metrics_list else "지표"
        return f"{base_text} 기준 {groupby_text}별 {metric_text} 분석으로 이해했습니다."

    return "포트폴리오 분석 요청으로 이해했습니다."


def build_analysis_answer(retrieved: Dict[str, Any], interpretation: str) -> str:
    atype = retrieved.get("analysis_type")
    lines: List[str] = ["[해석]", interpretation, ""]

    if atype == "share":
        metric = retrieved.get("metric", "commitment")
        metric_label_map = {
            "commitment": "약정액", "called": "콜금액", "outstanding": "투자잔액",
            "nav": "NAV", "count": "건수", "irr_avg": "평균 IRR",
            "irr_weighted_commitment": "약정가중 IRR", "irr_weighted_called": "콜금액가중 IRR",
            "irr_weighted_outstanding": "잔액가중 IRR", "irr_weighted_nav": "NAV가중 IRR",
        }

        base_value = retrieved.get("base_value")
        target_value = retrieved.get("target_value")
        ratio = retrieved.get("ratio")

        lines.append("[핵심 요약]")
        if ratio is None:
            lines.append("비중을 계산할 수 없습니다. (모수 값 없음)")
            return "\n".join(lines)

        lines.append(f"전체 대비 비중: {format_pct(ratio)} ({metric_label_map.get(metric, metric)} 기준)")
        lines.append("")
        lines.append("[세부]")

        def _fmt(v):
            if metric == "count":
                return f"{int(v or 0)}건"
            if metric.startswith("irr_"):
                return format_pct(v)
            return format_amount_uk(v)

        lines.append(f"- 전체: {_fmt(base_value)} / {retrieved.get('base_project_count', 0)}건")
        lines.append(f"- 대상: {_fmt(target_value)} / {retrieved.get('target_project_count', 0)}건")
        return "\n".join(lines)

    if atype == "grouped_metric":
        rows = retrieved.get("rows", []) or []
        metrics = retrieved.get("metrics", [])
        groupby = retrieved.get("groupby", [])

        lines.append("[핵심 요약]")
        if not rows:
            lines.append("조건에 맞는 분석 결과가 없습니다.")
            return "\n".join(lines)

        groupby_label_map = {
            "asset_class": "자산군", "region": "지역", "strategy": "전략",
            "manager": "운용사", "sector": "섹터", "vintage": "Vintage", "maturity_year": "만기",
        }
        metric_label_map = {
            "commitment": "약정", "called": "콜금액", "outstanding": "잔액", "nav": "NAV",
            "count": "건수", "irr_avg": "평균IRR",
            "irr_weighted_commitment": "IRR(약정가중)", "irr_weighted_called": "IRR(콜가중)",
            "irr_weighted_outstanding": "IRR(잔액가중)", "irr_weighted_nav": "IRR(NAV가중)",
        }

        group_header = " | ".join([groupby_label_map.get(g, g) for g in groupby])
        metric_header = " | ".join([metric_label_map.get(m, m) for m in metrics])

        lines += [f"{group_header} 기준 분석", f"지표: {metric_header}", "", "[분석 결과]"]

        for idx, r in enumerate(rows, start=1):
            group_text = " | ".join(r.get("group", []))
            parts = []
            for m in metrics:
                val = r.get(m)
                if m == "count":
                    parts.append(f"{int(val or 0)}건")
                elif m.startswith("irr_"):
                    parts.append(format_pct(val))
                else:
                    parts.append(format_amount_uk(val))
            lines.append(f"{idx}. {group_text} | {' | '.join(parts)} | {r.get('project_count', 0)}건")

        total = retrieved.get("base_project_count")
        if total:
            lines += ["", f"[총 투자건수] {total}건"]
        return "\n".join(lines)

    lines += ["[핵심 요약]", "분석 결과를 생성하지 못했습니다."]
    return "\n".join(lines)
