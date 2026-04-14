import threading
from typing import Optional, Tuple

from app import config
from app.util import get_kst_today_str

_LOCK = threading.Lock()
_STATE: dict = {"date": "", "users": {}}


def _reset_if_new_day() -> None:
    today = get_kst_today_str()
    if _STATE["date"] != today:
        _STATE["date"] = today
        _STATE["users"] = {}


def check_and_increment(sender_user_id: Optional[int], limit: int = None) -> Tuple[bool, int]:
    if sender_user_id is None:
        return True, 0

    if config.OWNER_CHAT_ID and str(sender_user_id) == str(config.OWNER_CHAT_ID):
        return True, 0

    lim = limit if limit is not None else config.DAILY_QUESTION_LIMIT

    with _LOCK:
        _reset_if_new_day()
        users = _STATE["users"]
        key = str(sender_user_id)
        used = int(users.get(key, 0))
        if used >= lim:
            return False, used
        users[key] = used + 1
        return True, used + 1
