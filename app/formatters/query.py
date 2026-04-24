from typing import Any, Dict, List

from app import config
from app.constants import OVERSEAS_REGIONS
from app.util import format_amount_uk, format_pct


def _humanize_filter_summary(filters: Dict[str, Any]) -> List[str]:
    parts: List[str] = []

    asset_class = filters.get("asset_class") or []
    if asset_class:
        parts.append(f"자산군={','.join(asset_class)}")

    region = filters.get("region") or []
    if region:
        if set(region) == set(OVERSEAS_REGIONS):
            parts.append("지역=해외(KOR 제외)")
        else:
            parts.append(f"지역={','.join(region)}")

    if filters.get("manager"):
        parts.append(f"운용사={','.join(filters['manager'])}")
    if filters.get("strategy"):
        parts.append(f"전략={'+'.join(filters['strategy'])}")
    if filters.get("sector"):
        parts.append(f"섹터={','.join(filters['sector'])}")
    if filters.get("project_id"):
        parts.append(f"Project_ID={','.join(filters['project_id'])}")
    if filters.get("fund_name_keywords"):
        parts.append(f"펀드명키워드={','.join(filters['fund_name_keywords'])}")
    if filters.get("asset_name_keywords"):
        parts.append(f"자산명키워드={','.join(filters['asset_name_keywords'])}")

    for min_key, max_key, label in [
        ("vintage_from", "vintage_to", "Vintage"),
        ("maturity_year_from", "maturity_year_to", "만기년도"),
        ("irr_min", "irr_max", "IRR"),
        ("commit_min", "commit_max", "약정"),
        ("called_min", "called_max", "콜금액"),
        ("outstanding_min", "outstanding_max", "투자잔액"),
        ("nav_min", "nav_max", "NAV"),
    ]:
        vmin, vmax = filters.get(min_key), filters.get(max_key)
        if vmin is None and vmax is None:
            continue
        if vmin is not None and vmax is not None:
            parts.append(f"{label}={vmin}~{vmax}")
        elif vmin is not None:
            parts.append(f"{label}>={vmin}")
        else:
            parts.append(f"{label}<={vmax}")

    return parts


def summarize_query_json(query_json: Dict[str, Any]) -> str:
    filters = query_json.get("filters", {}) or {}
    sort = query_json.get("sort", {}) or {}
    limit = int(query_json.get("output", {}).get("limit", config.DEFAULT_LIMIT) or config.DEFAULT_LIMIT)

    parts = _humanize_filter_summary(filters)

    sort_label_map = {
        "irr": "IRR",
        "commitment": "약정",
        "called": "콜금액",
        "outstanding": "투자잔액",
        "nav": "NAV",
        "maturity_year": "만기년도",
    }
    if sort.get("by"):
        direction = "오름차순" if sort.get("order") == "asc" else "내림차순"
        parts.append(f"정렬={sort_label_map.get(sort['by'], sort['by'])} {direction}")

    if limit != config.DEFAULT_LIMIT:
        parts.append(f"표시건수={limit}")

    if not parts:
        return "전체 포트폴리오 기준 조회로 이해했습니다."
    return f"{', '.join(parts)} 조건으로 조회했습니다."


def build_search_answer(retrieved: Dict[str, Any], interpretation: str) -> str:
    summary = retrieved["summary"]
    rows = retrieved["rows"]

    lines: List[str] = ["[해석]", interpretation, ""]

    if summary["count_projects_total"] == 0:
        lines += ["[핵심 요약]", "조건에 맞는 투자건이 없습니다.", "", "[리스트]", "-"]
        return "\n".join(lines)

    lines.append("[핵심 요약]")
    lines.append(
        f"조건에 맞는 투자건은 총 {summary['count_projects_total']}건입니다. "
        f"총 약정액 {format_amount_uk(summary['sum_commitment'])}, "
        f"누적 인출액 {format_amount_uk(summary['sum_called'])}, "
        f"현재 투자잔액 {format_amount_uk(summary['sum_outstanding'])}, "
        f"NAV는 {format_amount_uk(summary['sum_nav'])}입니다."
    )
    lines.append(f"가중평균 IRR은 {format_pct(summary['avg_irr'])}입니다.")
    lines.append("")
    lines.append("[리스트]")

    for idx, r in enumerate(rows, start=1):
        extra = []
        if r.get("Manager"):
            extra.append(str(r["Manager"]))
        if r.get("Region"):
            extra.append(str(r["Region"]))
        if r.get("Asset_Class"):
            extra.append(str(r["Asset_Class"]))
        if r.get("Strategy"):
            extra.append(str(r["Strategy"]))
        if r.get("Sector"):
            extra.append(str(r["Sector"]))
        if r.get("Vintage") is not None:
            extra.append(f"Vintage: {int(r['Vintage'])}")
        if r.get("Maturity_Year") is not None:
            extra.append(f"만기: {int(r['Maturity_Year'])}")
        if r.get("IRR") is not None:
            extra.append(f"IRR: {format_pct(r['IRR'])}")
        if r.get("NAV") is not None:
            extra.append(f"NAV: {format_amount_uk(r['NAV'])}")
        if r.get("Sub_Asset_Count"):
            extra.append(f"LT {int(r['Sub_Asset_Count'])}자산")

        tail = f" ({' | '.join(extra)})" if extra else ""
        lines.append(f"{idx}. {r['Project_ID']} | {r['Asset_Name']}{tail}")

    if rows:
        first = rows[0]
        lines += ["", "[상세]", f"/상세조회 {first['Project_ID']}"]
        if first.get("Sub_Asset_Count"):
            lines.append(f"/룩쓰루 {first['Project_ID']}")

    return "\n".join(lines)
