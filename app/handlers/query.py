import json
from typing import Any, Dict

from app import config
from app.db_engine import InvestmentDB
from app.formatters.query import build_search_answer, summarize_query_json
from app.logger import get_logger
from app.parsers.query import build_fixed_query_advice, parse_query
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


def handle_query_command(db: InvestmentDB, chat_id: int, raw: str, ctx: Dict[str, Any]) -> None:
    question = raw[len("/조회"):].strip()
    if not question:
        send_message(chat_id, "조회할 내용을 입력해 주세요. 예: /조회 미국 부동산 투자 현황")
        return

    if not _check_limit_or_reply(chat_id, ctx):
        return

    try:
        parsed = parse_query(question)
        if parsed.get("mode") == "advice":
            send_message(chat_id, parsed.get("advice_text") or build_fixed_query_advice())
            return

        query_json = parsed.get("query_json")
        if not query_json:
            send_message(chat_id, build_fixed_query_advice())
            return

        logger.info("query_json=%s", json.dumps(query_json, ensure_ascii=False))
        retrieved = db.search(query_json)
        interpretation = summarize_query_json(query_json)
        answer = build_search_answer(retrieved, interpretation)
        send_message(chat_id, answer)

        dialog_memory.set_context(chat_id, "query", query_json, interpretation)

    except Exception:
        logger.exception("query command failed")
        send_message(chat_id, "조회 처리 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.")


def handle_search_followup(db: InvestmentDB, chat_id: int, query_json: Dict[str, Any]) -> None:
    try:
        retrieved = db.search(query_json)
        interpretation = summarize_query_json(query_json)
        answer = build_search_answer(retrieved, interpretation)
        send_message(chat_id, answer)
        dialog_memory.set_context(chat_id, "query", query_json, interpretation)
    except Exception:
        logger.exception("query followup failed")
        send_message(chat_id, "후속 조회 처리 중 오류가 발생했습니다.")
