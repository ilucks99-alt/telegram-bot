from typing import Any, Dict, List

from app import config
from app.db_engine import InvestmentDB
from app.formatters.detail import build_detail_answer
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
        extra = f" ({' | '.join(tail)})" if tail else ""
        lines.append(f"{i}. {c['project_id']} | {c['asset_name']}{extra}")
    lines.append("")
    lines.append("예: /상세조회 BS00000726")
    return "\n".join(lines)


def _execute_detail(db: InvestmentDB, chat_id: int, project_id: str) -> None:
    detail = db.project_detail(project_id)
    if detail is None:
        send_message(chat_id, f"{project_id} — 해당 Project_ID 를 찾을 수 없습니다.")
        return
    answer = build_detail_answer(detail)
    send_long_message(chat_id, answer)

    extras = {
        "project_id": project_id,
        "asset_name": detail.get("asset_name"),
    }
    dialog_memory.set_context(
        chat_id,
        "lookthrough",
        {"project_id": project_id},
        summary=f"{project_id} {detail.get('asset_name') or ''} 상세",
        extras=extras,
    )


def handle_detail(db: InvestmentDB, chat_id: int, raw: str, ctx: Dict[str, Any]) -> None:
    args = raw[len("/상세조회"):].strip()
    if not args:
        send_message(
            chat_id,
            "사용법: /상세조회 BS00000XXX\n"
            "또는 펀드명/운용사 키워드: /상세조회 한화 Global PE 1호",
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
        _execute_detail(db, chat_id, cands[0]["project_id"])
    except Exception:
        logger.exception("detail command failed | args=%s", args)
        send_message(chat_id, "상세조회 처리 중 오류가 발생했습니다.")
