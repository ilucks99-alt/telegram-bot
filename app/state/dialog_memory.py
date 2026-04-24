import threading
import time
from typing import Any, Dict, Optional

from app import config

_LOCK = threading.Lock()
_STORE: Dict[str, Dict[str, Any]] = {}


def set_context(
    chat_id,
    kind: str,
    payload: Dict[str, Any],
    summary: str = "",
    extras: Optional[Dict[str, Any]] = None,
) -> None:
    """
    kind: 'query' | 'analysis' | 'lookthrough' | 'exposure'
    payload: the minimal re-execution payload for this kind
    summary: short human-readable description for LLM context
    extras: optional side data to help followup parser (e.g. rows/holdings/funds list for index resolution)
    """
    with _LOCK:
        _STORE[str(chat_id)] = {
            "kind": kind,
            "payload": payload,
            "summary": summary,
            "extras": extras or {},
            "ts": time.time(),
        }


def get_context(chat_id) -> Optional[Dict[str, Any]]:
    with _LOCK:
        entry = _STORE.get(str(chat_id))
        if not entry:
            return None
        if (time.time() - entry["ts"]) > config.DIALOG_MEMORY_TTL_SECONDS:
            _STORE.pop(str(chat_id), None)
            return None
        return entry


def clear_context(chat_id) -> None:
    with _LOCK:
        _STORE.pop(str(chat_id), None)


def touch(chat_id) -> None:
    with _LOCK:
        entry = _STORE.get(str(chat_id))
        if entry:
            entry["ts"] = time.time()
