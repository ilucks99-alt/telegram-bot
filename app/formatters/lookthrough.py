from typing import Any, Dict, List

from app.util import format_amount_uk, format_pct


def _fmt_share(v) -> str:
    if v is None:
        return "N/A"
    return f"{v * 100:.1f}%"


def _fmt_coupon(v) -> str:
    if v is None:
        return "N/A"
    return f"{v:.2f}%"


def build_lookthrough_answer(summ: Dict[str, Any]) -> str:
    lines: List[str] = []

    pid = summ.get("project_id", "")
    name = summ.get("asset_name", "") or "(이름 없음)"
    lines.append(f"[룩쓰루] {pid} | {name}")
    meta_parts = []
    if summ.get("asset_class"):
        meta_parts.append(str(summ["asset_class"]))
    if summ.get("region"):
        meta_parts.append(str(summ["region"]))
    if summ.get("manager"):
        meta_parts.append(f"운용사: {summ['manager']}")
    if summ.get("currency"):
        meta_parts.append(f"약정통화: {summ['currency']}")
    if meta_parts:
        lines.append(" | ".join(meta_parts))

    fund_line_parts = []
    if summ.get("fund_commitment") is not None:
        fund_line_parts.append(f"약정 {format_amount_uk(summ['fund_commitment'])}")
    if summ.get("fund_outstanding") is not None:
        fund_line_parts.append(f"잔액 {format_amount_uk(summ['fund_outstanding'])}")
    if summ.get("fund_nav") is not None:
        fund_line_parts.append(f"NAV {format_amount_uk(summ['fund_nav'])}")
    if summ.get("fund_irr") is not None:
        fund_line_parts.append(f"펀드 IRR {format_pct(summ['fund_irr'])}")
    if summ.get("tranche_count", 0) > 1:
        fund_line_parts.append(f"트렌치 {summ['tranche_count']}")
    if fund_line_parts:
        lines.append(" | ".join(fund_line_parts))

    lt_count = int(summ.get("lt_count") or 0)
    if lt_count == 0:
        lines += [
            "",
            "[룩쓰루 데이터 없음]",
            "이 펀드는 LookThrough에 하위자산이 등록되어 있지 않습니다.",
            "(실물·대출 직접보유 또는 대표 트렌치ID와 LT 키가 매칭되지 않는 경우)",
        ]
        return "\n".join(lines)

    lines.append("")
    lines.append(f"[하위자산 개요] 총 {lt_count:,}건, 장부합계 {format_amount_uk(summ.get('lt_book_total'))}")

    # 자산유형 mix
    subtypes = summ.get("subtype_share") or []
    if subtypes:
        lines.append("")
        lines.append("[자산유형 mix]")
        for s in subtypes:
            lines.append(
                f"- {s['sub_type']}: {int(s['count']):,}건 / "
                f"{format_amount_uk(s['book'])} ({_fmt_share(s['share'])})"
            )

    # 통화 mix
    ccys = summ.get("currency_share") or []
    if ccys:
        lines.append("")
        lines.append("[통화 익스포저]")
        for c in ccys:
            lines.append(f"- {c['currency']}: {format_amount_uk(c['book'])} ({_fmt_share(c['share'])})")

    # 가중평균 보유금리
    wc = summ.get("weighted_coupon")
    if wc is not None:
        lines.append("")
        lines.append(f"[가중평균 보유금리] {_fmt_coupon(wc)} (대출/채권, 장부가 가중)")

    # 만기 사다리
    mb = summ.get("maturity_buckets") or {}
    if any(mb.values()):
        lines.append("")
        lines.append("[만기 사다리]")
        lines.append(
            f"- 1년 내: {format_amount_uk(mb.get('<=1y'))} | "
            f"1~3년: {format_amount_uk(mb.get('1-3y'))} | "
            f"3년+: {format_amount_uk(mb.get('3y+'))} | "
            f"미정/만기없음: {format_amount_uk(mb.get('no_maturity'))}"
        )

    # TOP holdings
    tops = summ.get("top_holdings") or []
    if tops:
        lines.append("")
        lines.append(f"[TOP {len(tops)} holdings]")
        for i, h in enumerate(tops, 1):
            name_str = h.get("name") or "-"
            if len(name_str) > 40:
                name_str = name_str[:40] + "…"
            extra = [h.get("sub_type") or "-", h.get("currency") or "-"]
            if h.get("coupon") is not None:
                extra.append(_fmt_coupon(h["coupon"]))
            lines.append(
                f"{i}. {name_str} | {format_amount_uk(h.get('book'))} "
                f"({' | '.join(extra)})"
            )

    return "\n".join(lines)


def build_exposure_answer(exp: Dict[str, Any]) -> str:
    mode = exp.get("mode", "holding")
    mode_label = "발행인" if mode == "counterparty" else "종목/발행인(포괄)"
    q = exp.get("query", "")
    lines: List[str] = [f"[익스포저] '{q}' — 검색 대상: {mode_label}"]

    n_rows = int(exp.get("match_lt_rows") or 0)
    if n_rows == 0:
        lines += ["", "매칭 결과 없음."]
        return "\n".join(lines)

    match_book = exp.get("match_book") or 0.0
    org_total = exp.get("org_lt_total") or 0.0
    share = exp.get("share")
    fund_n = int(exp.get("fund_count") or 0)
    unmatched_n = int(exp.get("unmatched_fund_count") or 0)

    lines.append(
        f"매칭 LT {n_rows:,}건 | 노출합계 {format_amount_uk(match_book)} | "
        f"조직 LT 대비 {_fmt_share(share)}"
    )
    fund_txt = f"노출 펀드 {fund_n}개"
    if unmatched_n:
        fund_txt += f" (+ Dataset 미매칭 {unmatched_n}개)"
    lines.append(fund_txt)

    by_ccy = exp.get("by_currency") or []
    if by_ccy:
        ccy_parts = [f"{c['currency']} {format_amount_uk(c['book'])}" for c in by_ccy]
        lines.append("통화: " + " / ".join(ccy_parts))

    by_fund = exp.get("by_fund") or []
    if by_fund:
        lines.append("")
        lines.append(f"[펀드별 노출 TOP {len(by_fund)}]")
        for i, f in enumerate(by_fund, 1):
            pid = f.get("project_id") or "-"
            name = f.get("asset_name") or "-"
            if len(name) > 40:
                name = name[:40] + "…"
            tail_parts = []
            if f.get("manager"):
                tail_parts.append(str(f["manager"]))
            if f.get("asset_class"):
                tail_parts.append(str(f["asset_class"]))
            tail = f" ({' | '.join(tail_parts)})" if tail_parts else ""
            prefix = f"{pid} | " if pid else ""
            lines.append(
                f"{i}. {prefix}{name}{tail} | "
                f"{format_amount_uk(f.get('lt_book'))} | {int(f.get('lt_count') or 0)}건"
            )

    if fund_n == 1 and by_fund:
        pid = by_fund[0].get("project_id")
        if pid:
            lines += ["", "[상세]", f"/룩쓰루 {pid}"]

    return "\n".join(lines)
