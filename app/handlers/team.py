from app.logger import get_logger
from app.services import sheets
from app.services.telegram import send_message

logger = get_logger(__name__)


def handle_register_command(chat_id, raw: str) -> None:
    name = raw.replace("/등록", "", 1).strip()
    if not name:
        send_message(chat_id, "형식: /등록 홍길동")
        return

    try:
        sheets.register_member(name, chat_id)
        send_message(chat_id, f"{name} 등록이 완료되었습니다.")
    except Exception:
        logger.exception("register failed")
        send_message(chat_id, "등록 처리 중 오류가 발생했습니다.")
