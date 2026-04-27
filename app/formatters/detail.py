from typing import Any, Dict, List

from app.util import format_amount_uk, format_pct, normalize_text

_DIVIDER = "─" * 30


def _fmt_share(v) -> str:
    if v is None:
        return "N/A"
    return f"{v * 100:.1f}%"


def _fmt_multiple(v) -> str:
    if v is None:
        return "N/A"
    return f"{v:.2f}x"


def _fmt_coupon(v) -> str:
    if v is None:
        return "N/A"
    return f"{v:.2f}%"


def build_detail_answer(d: Dict[str, Any]) -> str:
    lines: List[str] = []

    pid = d.get("project_id", "")
    name = d.get("asset_name") or "(이름 없음)"
    en = d.get("asset_name_en") or ""
    head = f"[상세조회] {pid} | {name}"
    if en and normalize_text(en) != normalize_text(name):
        head += f"  ({en})"
    lines.append(head)
    lines.append(_DIVIDER)

    # 기본정보
    lines.append("[기본정보]")
    lines.append(f"- 자산군: {d.get('asset_class_std') or d.get('asset_class') or '-'}")
    lines.append(f"- 지역: {d.get('region_std') or d.get('region') or '-'}")
    lines.append(f"- 운용사: {d.get('manager') or '-'}")
    if d.get("strategy"):
        lines.append(f"- 전략: {d['strategy']}")
    if d.get("sector"):
        lines.append(f"- 섹터: {d['sector']}")
    if d.get("investment_type"):
        lines.append(f"- 투자유형: {d['investment_type']}")
    if d.get("detail_type"):
        lines.append(f"- 세부유형: {d['detail_type']}")
    if d.get("capital_structure"):
        lines.append(f"- 자본구조: {d['capital_structure']}")
    lines.append(f"- 약정통화: {d.get('currency') or '-'}")
    if d.get("vintage") is not None:
        lines.append(f"- 빈티지: {int(d['vintage'])}")
    if d.get("initial_date"):
        lines.append(f"- 최초인출일: {d['initial_date']}")
    if d.get("maturity_date"):
        lines.append(f"- 만기일: {d['maturity_date']}")

    # 규모·집행
    lines.append("")
    lines.append("[규모·집행]")
    drawdown_tail = (
        f" (Drawdown {d['drawdown'] * 100:.1f}%)"
        if d.get("drawdown") is not None else ""
    )
    lines.append(f"- 약정 (Commitment):  {format_amount_uk(d.get('commitment'))}")
    lines.append(f"- 누적실행 (Called):  {format_amount_uk(d.get('called'))}{drawdown_tail}")
    lines.append(f"- 누적상환 (Repaid):  {format_amount_uk(d.get('repaid'))}")
    lines.append(f"- 미인출 (Unfunded):  {format_amount_uk(d.get('unfunded'))}")
    lines.append(f"- 잔액 (Outstanding): {format_amount_uk(d.get('outstanding'))}")
    lines.append(f"- NAV (평가):         {format_amount_uk(d.get('nav'))}")

    # 성과지표
    lines.append("")
    lines.append("[성과지표]")
    lines.append(f"- IRR (대표): {format_pct(d.get('irr'))}")
    lines.append(f"- DPI:        {_fmt_multiple(d.get('dpi'))}")
    lines.append(f"- TVPI:       {_fmt_multiple(d.get('tvpi'))}")

    # 구조
    lines.append("")
    lines.append("[구조]")
    sub_n = int(d.get("sub_asset_count") or 0)
    lt_yes = "✓" if sub_n > 0 else "✗"
    lines.append(f"- 트렌치 수:    {int(d.get('tranche_count') or 0)}")
    lines.append(f"- 하위자산 수:  {sub_n}  (룩쓰루 가능 {lt_yes})")
    if d.get("sub_asset_key") is not None:
        lines.append(f"- SubAsset_Key: {d['sub_asset_key']}")

    # 룩쓰루
    lt = d.get("lookthrough")
    lt_count = int((lt or {}).get("lt_count") or 0)
    lines.append("")
    lines.append(_DIVIDER)

    if not lt or lt_count == 0:
        if sub_n > 0:
            lines.append("[룩쓰루] 하위자산은 등록되어 있으나 LT 키가 매칭되지 않습니다.")
        else:
            lines.append("[룩쓰루] 데이터 없음 (실물·대출 직접보유 또는 LT 미적재)")
        return "\n".join(lines)

    lines.append(
        f"[룩쓰루] 총 {lt_count:,}건 / 장부합계 {format_amount_uk(lt.get('lt_book_total'))}"
    )

    subs = lt.get("subtype_share") or []
    if subs:
        lines.append("")
        lines.append("[자산유형 mix]")
        for s in subs:
            lines.append(
                f"- {s['sub_type']}: {int(s['count']):,}건 / "
                f"{format_amount_uk(s['book'])} ({_fmt_share(s.get('share'))})"
            )

    ccys = lt.get("currency_share") or []
    if ccys:
        lines.append("")
        lines.append("[통화 익스포저]")
        for c in ccys:
            lines.append(
                f"- {c['currency']}: {format_amount_uk(c['book'])} "
                f"({_fmt_share(c.get('share'))})"
            )

    wc = lt.get("weighted_coupon")
    if wc is not None:
        lines.append("")
        lines.append(f"[가중평균 보유금리] {_fmt_coupon(wc)} (대출/채권, 장부가 가중)")

    mb = lt.get("maturity_buckets") or {}
    if any(mb.values()):
        lines.append("")
        lines.append("[만기 사다리]")
        lines.append(
            f"- 1년 내: {format_amount_uk(mb.get('<=1y'))} | "
            f"1~3년: {format_amount_uk(mb.get('1-3y'))} | "
            f"3년+: {format_amount_uk(mb.get('3y+'))} | "
            f"미정/만기없음: {format_amount_uk(mb.get('no_maturity'))}"
        )

    tops = lt.get("top_holdings") or []
    if tops:
        shown = tops[:5]
        lines.append("")
        lines.append(f"[TOP {len(shown)} holdings]")
        for i, h in enumerate(shown, 1):
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
        if len(tops) > 5:
            lines.append(f"… (외 {len(tops) - 5}건; /룩쓰루 {pid} 로 전체 확인)")

    return "\n".join(lines)
