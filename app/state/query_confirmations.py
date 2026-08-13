import secrets
import threading
import time
from typing import Any, Dict, Optional

from app import config

_LOCK = threading.Lock()
_STORE: Dict[str, Dict[str, Any]] = {}


def create(chat_id: int, query_json: Dict[str, Any]) -> str:
    token = secrets.token_urlsafe(6)
    with _LOCK:
        _STORE[token] = {
            "chat_id": str(chat_id),
            "query_json": query_json,
            "ts": time.time(),
        }
    return token


def pop(token: str, chat_id: int) -> Optional[Dict[str, Any]]:
    with _LOCK:
        entry = _STORE.pop(token, None)
    if not entry or entry["chat_id"] != str(chat_id):
        return None
    if time.time() - entry["ts"] > config.DIALOG_MEMORY_TTL_SECONDS:
        return None
    return entry
