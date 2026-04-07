import config
import os
import requests
import time
import shutil
from typing import Optional, List


# =========================================================
# Telegram API
# =========================================================
BASE_URL = f"https://api.telegram.org/bot{config.TELEGRAM_TOKEN}"


def telegram_get(method: str, params: Optional[dict] = None, timeout: int = 60) -> dict:
    url = f"{BASE_URL}/{method}"
    last_err = None
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            if not data.get("ok"):
                raise RuntimeError(f"Telegram API 오류: {data}")
            return data
        except Exception as e:
            last_err = e
            if attempt < 2:
                time.sleep(attempt + 1)
            else:
                raise last_err


def telegram_post(method: str, data: Optional[dict] = None, files: Optional[dict] = None, timeout: int = 60) -> dict:
    url = f"{BASE_URL}/{method}"
    last_err = None
    for attempt in range(3):
        try:
            r = requests.post(url, data=data, files=files, timeout=timeout)
            r.raise_for_status()
            payload = r.json()
            if not payload.get("ok"):
                raise RuntimeError(f"Telegram API 오류: {payload}")
            return payload
        except Exception as e:
            last_err = e
            if attempt < 2:
                time.sleep(attempt + 1)
            else:
                raise last_err


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


def send_message(chat_id: int, text: str) -> None:
    for chunk in split_text(text, limit=3900):
        telegram_post(
            "sendMessage",
            data={
                "chat_id": chat_id,
                "text": chunk
            }
        )


def send_document(chat_id: int, file_path: str, caption: str = "") -> None:
    with open(file_path, "rb") as f:
        telegram_post(
            "sendDocument",
            data={
                "chat_id": chat_id,
                "caption": caption[:1024]
            },
            files={"document": f}
        )


def get_updates(offset: Optional[int] = None, timeout: int = 50) -> List[dict]:
    params = {"timeout": timeout}
    if offset is not None:
        params["offset"] = offset
    data = telegram_get("getUpdates", params=params, timeout=timeout + 10)
    return data.get("result", [])


def load_offset() -> Optional[int]:
    if not os.path.exists(config.OFFSET_FILE):
        return None
    try:
        with open(config.OFFSET_FILE, "r", encoding="utf-8") as f:
            return int(f.read().strip())
    except Exception:
        return None


def save_offset(offset: int) -> None:
    tmp = config.OFFSET_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(str(offset))
    shutil.move(tmp, config.OFFSET_FILE)


def send_long_message(chat_id, text, chunk_size=3500):
    if not text:
        return

    text = str(text)
    for i in range(0, len(text), chunk_size):
        send_message(chat_id, text[i:i + chunk_size])


def get_file_info(file_id: str) -> dict:
    url = f"https://api.telegram.org/bot{config.TELEGRAM_TOKEN}/getFile"
    resp = requests.get(url, params={"file_id": file_id}, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    if not data.get("ok"):
        raise RuntimeError(f"getFile 실패: {data}")

    return data["result"]


def download_telegram_file(file_id: str, file_name: str | None = None) -> str:
    file_info = get_file_info(file_id)
    file_path_on_tg = file_info["file_path"]
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