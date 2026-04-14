import json
from typing import Any, Dict

from app import config
from app.db_engine import InvestmentDB
from app.formatters.analysis import build_analysis_answer, summarize_analysis_json
from app.logger import get_logger
from app.parsers.analysis import build_fixed_analysis_advice, parse_analysis
from app.services.telegram import send_message
from app.state import dialog_memory, question_limit
from app.util import get_sender_display_name

logger = get_logger(__name__)


def _check_limit_or_reply(chat_id: int, ctx: Dict[str, Any]) -> bool:
    sender = ctx.get("sender_user_id")
    allowed, _ = question_limit.check_and_increment(sender, config.DAILY_QUESTION_LIMIT)
    if not allowed:
        name = get_sender_display_name(ctx)
        send_message(chat_id, f"{name}님은 오늘 조회/분석 한도({config.DAILY_QUESTION_LIMIT}회)를 모두 사용했습니다.")
        return False
    return True


def handle_analysis_command(db: InvestmentDB, chat_id: int, raw: str, ctx: Dict[str, Any]) -> None:
    question = raw[len("/분석"):].strip()
    if not question:
        send_message(chat_id, "분석할 내용을 입력해 주세요. 예: /분석 전체 포트폴리오에서 미국 비중")
        return

    if not _check_limit_or_reply(chat_id, ctx):
        return

    try:
        parsed = parse_analysis(question)
        if parsed.get("mode") == "advice":
            send_message(chat_id, parsed.get("advice_text") or build_fixed_analysis_advice())
            return

        analysis_json = parsed.get("analysis_json")
        if not analysis_json:
            send_message(chat_id, build_fixed_analysis_advice())
            return

        logger.info("analysis_json=%s", json.dumps(analysis_json, ensure_ascii=False))
        retrieved = db.analyze(analysis_json)
        interpretation = summarize_analysis_json(analysis_json)
        answer = build_analysis_answer(retrieved, interpretation)
        send_message(chat_id, answer)

        dialog_memory.set_context(chat_id, "analysis", analysis_json, interpretation)

    except Exception:
        logger.exception("analysis command failed")
        send_message(chat_id, "분석 처리 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.")


def handle_analysis_followup(db: InvestmentDB, chat_id: int, analysis_json: Dict[str, Any]) -> None:
    try:
        retrieved = db.analyze(analysis_json)
        interpretation = summarize_analysis_json(analysis_json)
        answer = build_analysis_answer(retrieved, interpretation)
        send_message(chat_id, answer)
        dialog_memory.set_context(chat_id, "analysis", analysis_json, interpretation)
    except Exception:
        logger.exception("analysis followup failed")
        send_message(chat_id, "후속 분석 처리 중 오류가 발생했습니다.")
