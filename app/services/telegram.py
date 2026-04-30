import json
import os
import time
from typing import List, Optional, Union

import requests

from app import config
from app.logger import get_logger

logger = get_logger(__name__)

ChatId = Union[int, str]


def _base_url() -> str:
    return f"https://api.telegram.org/bot{config.TELEGRAM_TOKEN}"


def _handle_rate_limit(resp: requests.Response) -> Optional[float]:
    if resp.status_code != 429:
        return None
    try:
        data = resp.json()
        retry_after = (data.get("parameters") or {}).get("retry_after")
        if retry_after:
            return float(retry_after)
    except Exception:
        pass
    header = resp.headers.get("Retry-After")
    if header:
        try:
            return float(header)
        except ValueError:
            pass
    return 1.0


def _telegram_request(method: str, http_method: str, **kwargs) -> dict:
    url = f"{_base_url()}/{method}"
    last_err: Optional[Exception] = None

    for attempt in range(3):
        try:
            resp = requests.request(http_method, url, timeout=kwargs.pop("timeout", 60), **kwargs)

            if resp.status_code == 429:
                wait = _handle_rate_limit(resp) or (attempt + 1)
                logger.warning("Telegram 429, sleeping %.1fs", wait)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            data = resp.json()
            if not data.get("ok"):
                raise RuntimeError(f"Telegram API error: {data}")
            return data
        except Exception as e:
            last_err = e
            if attempt < 2:
                time.sleep(attempt + 1)

    assert last_err is not None
    raise last_err


def telegram_get(method: str, params: Optional[dict] = None, timeout: int = 60) -> dict:
    return _telegram_request(method, "GET", params=params, timeout=timeout)


def telegram_post(method: str, data: Optional[dict] = None, files: Optional[dict] = None, timeout: int = 60) -> dict:
    return _telegram_request(method, "POST", data=data, files=files, timeout=timeout)


def split_text(text: str, limit: int = 3900) -> List[str]:
    text = text or ""
    if len(text) <= limit:
        return [text]

    chunks: List[str] = []
    current = ""

    for line in text.splitlines(keepends=True):
        if len(line) > limit:
            if current:
                chunks.append(current)
                current = ""
            for i in range(0, len(line), limit):
                chunks.append(line[i:i + limit])
            continue

        if len(current) + len(line) > limit:
            chunks.append(current)
            current = line
        else:
            current += line

    if current:
        chunks.append(current)

    return chunks


def send_message(
    chat_id: ChatId,
    text: str,
    parse_mode: Optional[str] = None,
    disable_web_page_preview: bool = False,
) -> None:
    if chat_id is None:
        return
    for chunk in split_text(text, limit=3900):
        data = {"chat_id": str(chat_id), "text": chunk}
        if parse_mode:
            data["parse_mode"] = parse_mode
        if disable_web_page_preview:
            data["disable_web_page_preview"] = "true"
        telegram_post("sendMessage", data=data)


def send_message_with_keyboard(chat_id: ChatId, text: str, keyboard: dict) -> None:
    """
    Send a message with an inline keyboard. If text needs to be chunked,
    only the final chunk gets the keyboard so buttons don't get duplicated.
    """
    if chat_id is None:
        return
    chunks = split_text(text, limit=3900)
    if not chunks:
        return
    last = len(chunks) - 1
    for i, chunk in enumerate(chunks):
        data = {"chat_id": str(chat_id), "text": chunk}
        if i == last:
            data["reply_markup"] = json.dumps(keyboard)
        telegram_post("sendMessage", data=data)


def answer_callback_query(callback_query_id: str, text: str = "") -> None:
    if not callback_query_id:
        return
    data = {"callback_query_id": callback_query_id}
    if text:
        data["text"] = text[:200]
    try:
        telegram_post("answerCallbackQuery", data=data)
    except Exception:
        logger.exception("answerCallbackQuery failed")


def edit_message_text(
    chat_id: ChatId,
    message_id: int,
    text: str,
    reply_markup: Optional[dict] = None,
) -> None:
    """Edit a previously sent message. Removes the inline keyboard if reply_markup is None."""
    if chat_id is None or message_id is None:
        return
    data = {
        "chat_id": str(chat_id),
        "message_id": int(message_id),
        "text": text[:3900],
    }
    if reply_markup is not None:
        data["reply_markup"] = json.dumps(reply_markup)
    try:
        telegram_post("editMessageText", data=data)
    except Exception:
        logger.exception("editMessageText failed")


def send_long_message(
    chat_id: ChatId,
    text: str,
    chunk_size: int = 3500,
    parse_mode: Optional[str] = None,
    disable_web_page_preview: bool = False,
) -> None:
    # send_message 가 이미 split_text(limit=3900)로 줄 단위 안전 분할을 수행하므로
    # 여기서 추가 하드 청킹을 하면 길이 3500~3900 구간의 단일 보고가 불필요하게
    # 2개 메시지로 쪼개져 "완료 보고가 2번 온 것처럼" 보이는 문제가 생긴다.
    if not text or chat_id is None:
        return
    send_message(chat_id, str(text), parse_mode=parse_mode, disable_web_page_preview=disable_web_page_preview)


def send_document(chat_id: ChatId, file_path: str, caption: str = "") -> None:
    if chat_id is None:
        return
    with open(file_path, "rb") as f:
        telegram_post(
            "sendDocument",
            data={"chat_id": str(chat_id), "caption": caption[:1024]},
            files={"document": f},
        )


def set_webhook(url: str, secret_token: Optional[str] = None) -> dict:
    data = {"url": url}
    if secret_token:
        data["secret_token"] = secret_token
    return telegram_post("setWebhook", data=data)


def delete_webhook() -> dict:
    return telegram_post("deleteWebhook", data={})


def get_file_info(file_id: str) -> dict:
    return telegram_get("getFile", params={"file_id": file_id})["result"]


def download_telegram_file(file_id: str, file_name: Optional[str] = None) -> str:
    info = get_file_info(file_id)
    file_path_on_tg = info["file_path"]
    download_url = f"https://api.telegram.org/file/bot{config.TELEGRAM_TOKEN}/{file_path_on_tg}"

    os.makedirs(config.TELEGRAM_FILE_DIR, exist_ok=True)

    if not file_name:
        file_name = os.path.basename(file_path_on_tg)

    local_path = os.path.join(config.TELEGRAM_FILE_DIR, file_name)

    resp = requests.get(download_url, timeout=60)
    resp.raise_for_status()

    with open(local_path, "wb") as f:
        f.write(resp.content)

    return local_path
