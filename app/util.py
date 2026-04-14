import math
import re
from datetime import datetime
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import pandas as pd

KST = ZoneInfo("Asia/Seoul")


def normalize_text(s: Any) -> str:
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return ""
    try:
        if pd.isna(s):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(s).strip().lower()
    s = re.sub(r"[\s\-\_\(\)\[\]\{\}\.,/&|:;]+", "", s)
    return s


def safe_num(x: Any) -> Optional[float]:
    try:
        if pd.isna(x):
            return None
    except (TypeError, ValueError):
        pass
    try:
        val = float(x)
        if math.isnan(val):
            return None
        return val
    except (TypeError, ValueError):
        return None


def format_amount_uk(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    return f"{value:,.0f}억"


def format_pct(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    return f"{value * 100:.2f}%"


def contains_match_norm(norm_series: pd.Series, keyword: str) -> pd.Series:
    norm_kw = normalize_text(keyword)
    if not norm_kw:
        return pd.Series([False] * len(norm_series), index=norm_series.index)
    return norm_series.fillna("").astype(str).str.contains(re.escape(norm_kw), regex=True)


def get_sender_display_name(ctx: Dict[str, Any]) -> str:
    first_name = (ctx.get("sender_first_name") or "").strip()
    last_name = (ctx.get("sender_last_name") or "").strip()
    username = (ctx.get("sender_username") or "").strip()

    telegram_name = f"{last_name}{first_name}".strip() or first_name
    if telegram_name:
        return telegram_name
    if username:
        return f"@{username}"
    return "사용자"


def extract_message_context(update: dict) -> Dict[str, Any]:
    msg = update.get("message") or update.get("edited_message")
    if not msg:
        return {
            "chat_id": None,
            "text": None,
            "sender_user_id": None,
            "sender_first_name": None,
            "sender_last_name": None,
            "sender_username": None,
            "document": None,
        }

    chat = msg.get("chat", {}) or {}
    sender = msg.get("from", {}) or {}
    document = msg.get("document")

    doc_info = None
    if document:
        doc_info = {
            "file_id": document.get("file_id"),
            "file_name": document.get("file_name"),
            "mime_type": document.get("mime_type"),
            "file_size": document.get("file_size"),
        }

    return {
        "chat_id": chat.get("id"),
        "text": msg.get("text") or msg.get("caption") or "",
        "sender_user_id": sender.get("id"),
        "sender_first_name": sender.get("first_name"),
        "sender_last_name": sender.get("last_name"),
        "sender_username": sender.get("username"),
        "document": doc_info,
    }


def get_kst_today_str() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


def get_kst_now() -> datetime:
    return datetime.now(KST)


def get_kst_today_year() -> int:
    return int(datetime.now(KST).strftime("%Y"))


def now_ts() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
