from typing import Any, Dict, List

from app import config
from app.db_engine import InvestmentDB
from app.formatters.lookthrough import build_exposure_answer, build_lookthrough_answer
from app.logger import get_logger
from app.services.telegram import send_long_message, send_message
from app.state import dialog_memory, question_limit
from app.util import get_sender_display_name

logger = get_logger(__name__)


def _check_limit_or_reply(chat_id: int, ctx: Dict[str, Any]) -> bool:
    sender = ctx.get("sender_user_id")
    allowed, _ = question_limit.check_and_increment(sender, config.DAILY_QUESTION_LIMIT)
    if not allowed:
        name = get_sender_display_name(ctx)
        send_message(
            chat_id,
            f"{name}님은 오늘 조회/분석 한도({config.DAILY_QUESTION_LIMIT}회)를 모두 사용했습니다.",
        )
        return False
    return True


def _render_candidates(cands: List[Dict[str, Any]]) -> str:
    lines = ["해당 키워드로 여러 펀드가 매칭되었습니다. 더 구체적으로 입력해 주세요."]
    for i, c in enumerate(cands, 1):
        tail = []
        if c.get("manager"):
            tail.append(c["manager"])
        if c.get("asset_class"):
            tail.append(c["asset_class"])
        if c.get("sub_asset_count"):
            tail.append(f"LT {c['sub_asset_count']}자산")
        extra = f" ({' | '.join(tail)})" if tail else ""
        lines.append(f"{i}. {c['project_id']} | {c['asset_name']}{extra}")
    lines.append("")
    lines.append("예: /룩쓰루 BS00000726  또는  /룩쓰루 한화 Global PE 1호")
    return "\n".join(lines)


def _execute_lookthrough(db: InvestmentDB, chat_id: int, project_id: str) -> None:
    summ = db.lookthrough_summary(project_id)
    if summ is None:
        send_message(chat_id, f"{project_id} — 해당 Project_ID 를 찾을 수 없습니다.")
        return
    answer = build_lookthrough_answer(summ)
    send_long_message(chat_id, answer)

    # followup 용 컨텍스트 저장 — top holdings list 를 extras 로 같이 둠
    extras = {
        "project_id": project_id,
        "asset_name": summ.get("asset_name"),
        "top_holdings": [
            {"name": h.get("name"), "counterparty": h.get("counterparty")}
            for h in (summ.get("top_holdings") or [])[:10]
        ],
    }
    dialog_memory.set_context(
        chat_id,
        "lookthrough",
        {"project_id": project_id},
        summary=f"{project_id} {summ.get('asset_name') or ''} 룩쓰루",
        extras=extras,
    )


def _execute_exposure(db: InvestmentDB, chat_id: int, mode: str, query: str) -> None:
    exp = db.exposure_search(mode, query, fund_top_n=20)
    answer = build_exposure_answer(exp)
    send_long_message(chat_id, answer)

    extras = {
        "mode": mode,
        "query": query,
        "funds": [
            {"project_id": f.get("project_id") or "", "asset_name": f.get("asset_name") or ""}
            for f in (exp.get("by_fund") or [])[:20]
        ],
    }
    dialog_memory.set_context(
        chat_id,
        "exposure",
        {"mode": mode, "query": query},
        summary=f"'{query}' 익스포저 (mode={mode})",
        extras=extras,
    )


def handle_lookthrough_command(db: InvestmentDB, chat_id: int, raw: str, ctx: Dict[str, Any]) -> None:
    args = raw[len("/룩쓰루"):].strip()
    if not args:
        send_message(
            chat_id,
            "사용법: /룩쓰루 BS00000XXX\n"
            "또는 펀드명/운용사 키워드: /룩쓰루 한화 Global PE 1호",
        )
        return

    if not _check_limit_or_reply(chat_id, ctx):
        return

    try:
        cands = db.resolve_project_ref(args, limit=5)
        if not cands:
            send_message(
                chat_id,
                f"'{args}' 로 매칭되는 펀드가 없습니다.\n"
                "정확한 Project_ID(BS00000XXX) 또는 펀드명 키워드를 입력해 주세요.",
            )
            return
        if len(cands) > 1:
            send_message(chat_id, _render_candidates(cands))
            return
        _execute_lookthrough(db, chat_id, cands[0]["project_id"])
    except Exception:
        logger.exception("lookthrough command failed | args=%s", args)
        send_message(chat_id, "룩쓰루 처리 중 오류가 발생했습니다.")


def _parse_exposure_args(args: str) -> Dict[str, str]:
    """지원 형식:
      /익스포저 발행인 평택동부도로
      /익스포저 종목 PGIM
      /익스포저 평택동부도로              (기본: 포괄 매칭 = counterparty + holding)
    """
    s = args.strip()
    if not s:
        return {"mode": "", "query": ""}

    parts = s.split(maxsplit=1)
    head = parts[0].strip().lower()
    rest = parts[1].strip() if len(parts) > 1 else ""

    if head in ("발행인", "거래상대방", "counterparty", "cp"):
        return {"mode": "counterparty", "query": rest}
    if head in ("종목", "holding", "이름", "name"):
        return {"mode": "holding", "query": rest}
    # prefix 없이 들어오면 포괄 매칭
    return {"mode": "holding", "query": s}


def handle_exposure_command(db: InvestmentDB, chat_id: int, raw: str, ctx: Dict[str, Any]) -> None:
    args = raw[len("/익스포저"):].strip()
    parsed = _parse_exposure_args(args)
    mode = parsed["mode"]
    query = parsed["query"]

    if not query:
        send_message(
            chat_id,
            "사용법:\n"
            "  /익스포저 발행인 평택동부도로\n"
            "  /익스포저 종목 PGIM\n"
            "  /익스포저 키워드        (포괄 매칭)",
        )
        return

    if not _check_limit_or_reply(chat_id, ctx):
        return

    try:
        _execute_exposure(db, chat_id, mode, query)
    except Exception:
        logger.exception("exposure command failed | mode=%s query=%s", mode, query)
        send_message(chat_id, "익스포저 처리 중 오류가 발생했습니다.")


# =========================================================
# Followup dispatch (router._try_followup 에서 호출)
# =========================================================
def handle_lookthrough_followup(db: InvestmentDB, chat_id: int, payload: Dict[str, Any]) -> None:
    pid = (payload or {}).get("project_id") or ""
    if not pid:
        send_message(chat_id, "룩쓰루 대상 Project_ID 를 해석하지 못했습니다.")
        return
    try:
        _execute_lookthrough(db, chat_id, pid)
    except Exception:
        logger.exception("lookthrough followup failed | pid=%s", pid)
        send_message(chat_id, "룩쓰루 후속 처리 중 오류가 발생했습니다.")


def handle_exposure_followup(db: InvestmentDB, chat_id: int, payload: Dict[str, Any]) -> None:
    mode = (payload or {}).get("mode") or "holding"
    query = (payload or {}).get("query") or ""
    if not query:
        send_message(chat_id, "익스포저 키워드를 해석하지 못했습니다.")
        return
    if mode not in ("counterparty", "holding"):
        mode = "holding"
    try:
        _execute_exposure(db, chat_id, mode, query)
    except Exception:
        logger.exception("exposure followup failed | mode=%s query=%s", mode, query)
        send_message(chat_id, "익스포저 후속 처리 중 오류가 발생했습니다.")
