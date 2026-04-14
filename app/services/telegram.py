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


def send_message(chat_id: ChatId, text: str) -> None:
    if chat_id is None:
        return
    for chunk in split_text(text, limit=3900):
        telegram_post(
            "sendMessage",
            data={"chat_id": str(chat_id), "text": chunk},
        )


def send_long_message(chat_id: ChatId, text: str, chunk_size: int = 3500) -> None:
    if not text or chat_id is None:
        return
    text = str(text)
    for i in range(0, len(text), chunk_size):
        send_message(chat_id, text[i:i + chunk_size])


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
