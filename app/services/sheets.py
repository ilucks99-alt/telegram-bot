import base64
import json
import re
import threading
import time
from typing import Any, Dict, List, Optional

from app import config
from app.logger import get_logger
from app.util import now_ts

logger = get_logger(__name__)

try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    gspread = None
    Credentials = None

# =========================================================
# Constants
# =========================================================
TASKS_HEADERS = [
    "task_id", "assignee_name", "assignee_chat_id", "owner_chat_id",
    "instruction", "status", "feedback_round", "priority", "due_at",
    "project_id", "created_at", "updated_at", "last_activity_at",
    "closed_at", "final_report", "owner_reported_at", "due_reminder_sent",
    "acked_at", "unack_alert_sent",
]
TASK_HISTORY_HEADERS = ["task_id", "ts", "role", "text"]
MEMBERS_HEADERS = ["name", "chat_id", "registered_at"]
NEWS_DEDUP_HEADERS = ["slot_key", "sent_at"]

ACTIVE_STATUSES = {"waiting_for_reply", "feedback_sent", "processing_file", "reviewing"}


# =========================================================
# Internal state
# =========================================================
_client = None
_spreadsheet = None
_lock = threading.Lock()

_members_cache: Optional[Dict[str, str]] = None
_members_cache_at: float = 0.0
_MEMBERS_CACHE_TTL = 60.0

# Tasks 시트는 webhook 1건 처리 시 3~5번 읽힌다(has_active, get_by_assignee, get_overdue 등).
# 짧은 TTL 캐시로 동일 요청 burst 를 줄이고 Google Sheets read quota 를 아낀다.
# Mutation(create_task / update_task_fields) 시 즉시 무효화해서 stale 읽기를 막는다.
_tasks_cache: Optional[List[Dict[str, Any]]] = None
_tasks_cache_at: float = 0.0
_TASKS_CACHE_TTL = 15.0

# TaskHistory 도 동일 사유로 캐시. 단일 task 진행 중 history 가 3~5회 풀스캔되며
# 시트가 커질수록 한 호출만 1초+ 가 걸리기 시작한다. task_id 별 dict 로 묶어두고
# append 시점에 같은 task 의 캐시 엔트리에도 직접 push 해서 stale 을 막는다.
_history_cache: Optional[Dict[str, List[Dict[str, Any]]]] = None
_history_cache_at: float = 0.0
_HISTORY_CACHE_TTL = 30.0


def _load_sa_credentials() -> Optional["Credentials"]:
    if Credentials is None:
        return None
    raw = config.GOOGLE_SA_JSON.strip()
    if not raw:
        return None

    # Support either raw JSON or base64 encoded
    try:
        if raw.startswith("{"):
            sa_info = json.loads(raw)
        else:
            sa_info = json.loads(base64.b64decode(raw).decode("utf-8"))
    except Exception:
        logger.exception("Failed to parse GOOGLE_SA_JSON")
        return None

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    return Credentials.from_service_account_info(sa_info, scopes=scopes)


def is_available() -> bool:
    return gspread is not None and bool(config.GOOGLE_SA_JSON) and bool(config.GOOGLE_SHEET_ID)


def get_spreadsheet():
    global _client, _spreadsheet
    if _spreadsheet is not None:
        return _spreadsheet
    if not is_available():
        return None

    creds = _load_sa_credentials()
    if creds is None:
        return None

    _client = gspread.authorize(creds)
    _spreadsheet = _client.open_by_key(config.GOOGLE_SHEET_ID)
    return _spreadsheet


def _ensure_worksheet(sheet, title: str, headers: List[str]):
    try:
        ws = sheet.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=title, rows=1000, cols=max(20, len(headers)))
        ws.append_row(headers)
        return ws

    # Ensure headers row exists
    existing = ws.row_values(1)
    if existing != headers:
        # Only overwrite if empty or partial
        if not existing:
            ws.append_row(headers)
        elif len(existing) < len(headers):
            # Extend missing columns
            for i, h in enumerate(headers):
                if i >= len(existing) or existing[i] != h:
                    ws.update_cell(1, i + 1, h)
    return ws


def ensure_tabs_initialized(seed_members: Optional[Dict[str, str]] = None) -> None:
    sheet = get_spreadsheet()
    if sheet is None:
        logger.warning("Google Sheets not available, skipping tab init")
        return

    with _lock:
        _ensure_worksheet(sheet, "Tasks", TASKS_HEADERS)
        _ensure_worksheet(sheet, "TaskHistory", TASK_HISTORY_HEADERS)
        _ensure_worksheet(sheet, "NewsDedup", NEWS_DEDUP_HEADERS)
        members_ws = _ensure_worksheet(sheet, "Members", MEMBERS_HEADERS)

        if seed_members:
            existing = members_ws.get_all_records()
            existing_names = {r.get("name", "").strip() for r in existing}
            rows_to_add = []
            for name, chat_id in seed_members.items():
                if name.strip() and name.strip() not in existing_names:
                    rows_to_add.append([name.strip(), str(chat_id), now_ts()])
            if rows_to_add:
                members_ws.append_rows(rows_to_add, value_input_option="USER_ENTERED")
                logger.info("Seeded %d members into Sheets", len(rows_to_add))


# =========================================================
# Members
# =========================================================
def _tab(title: str):
    sheet = get_spreadsheet()
    if sheet is None:
        return None
    return sheet.worksheet(title)


def _invalidate_members_cache() -> None:
    global _members_cache, _members_cache_at
    _members_cache = None
    _members_cache_at = 0.0


def load_members(force: bool = False) -> Dict[str, str]:
    global _members_cache, _members_cache_at
    now = time.time()
    if not force and _members_cache is not None and (now - _members_cache_at) < _MEMBERS_CACHE_TTL:
        return _members_cache

    ws = _tab("Members")
    if ws is None:
        return {}

    records = ws.get_all_records()
    result = {}
    for r in records:
        name = (r.get("name") or "").strip()
        chat_id = str(r.get("chat_id") or "").strip()
        if name and chat_id:
            result[name] = chat_id

    _members_cache = result
    _members_cache_at = now
    return result


def register_member(name: str, chat_id) -> None:
    name = name.strip()
    if not name:
        return
    ws = _tab("Members")
    if ws is None:
        return

    records = ws.get_all_records()
    for idx, r in enumerate(records, start=2):
        if (r.get("name") or "").strip() == name:
            ws.update_cell(idx, 2, str(chat_id))
            ws.update_cell(idx, 3, now_ts())
            _invalidate_members_cache()
            return

    ws.append_row([name, str(chat_id), now_ts()], value_input_option="USER_ENTERED")
    _invalidate_members_cache()


def find_member_chat_id(name: str) -> Optional[str]:
    members = load_members()
    return members.get((name or "").strip())


# =========================================================
# Tasks
# =========================================================
def _row_to_dict(headers: List[str], row: List[str]) -> Dict[str, Any]:
    d: Dict[str, Any] = {}
    for i, h in enumerate(headers):
        d[h] = row[i] if i < len(row) else ""
    return d


def _invalidate_tasks_cache() -> None:
    global _tasks_cache, _tasks_cache_at
    _tasks_cache = None
    _tasks_cache_at = 0.0


def _read_all_tasks(force: bool = False) -> List[Dict[str, Any]]:
    global _tasks_cache, _tasks_cache_at
    now = time.time()
    if not force and _tasks_cache is not None and (now - _tasks_cache_at) < _TASKS_CACHE_TTL:
        return _tasks_cache

    ws = _tab("Tasks")
    if ws is None:
        return []
    values = ws.get_all_values()
    if not values:
        _tasks_cache = []
        _tasks_cache_at = now
        return _tasks_cache
    headers = values[0]
    result = [_row_to_dict(headers, row) for row in values[1:] if any(row)]
    _tasks_cache = result
    _tasks_cache_at = now
    return result


def _find_task_row_index(ws, task_id: str) -> int:
    values = ws.get_all_values()
    if not values:
        return -1
    for idx, row in enumerate(values[1:], start=2):
        if row and row[0] == task_id:
            return idx
    return -1


def create_task(
    task_id: str,
    assignee_name: str,
    assignee_chat_id,
    owner_chat_id,
    instruction: str,
    project_id: Optional[str] = None,
    initial_status: str = "waiting_for_reply",
    due_at: Optional[str] = None,
) -> Dict[str, Any]:
    ws = _tab("Tasks")
    if ws is None:
        raise RuntimeError("Sheets 'Tasks' 탭이 없습니다")

    ts = now_ts()
    task = {
        "task_id": task_id,
        "assignee_name": assignee_name,
        "assignee_chat_id": str(assignee_chat_id),
        "owner_chat_id": str(owner_chat_id),
        "instruction": instruction,
        "status": initial_status,
        "feedback_round": "0",
        "priority": "",
        "due_at": due_at or "",
        "project_id": project_id or "",
        "created_at": ts,
        "updated_at": ts,
        "last_activity_at": ts,
        "closed_at": "",
        "final_report": "",
        "owner_reported_at": "",
        "due_reminder_sent": "",
        "acked_at": "",
        "unack_alert_sent": "",
    }
    row = [task.get(h, "") for h in TASKS_HEADERS]
    ws.append_row(row, value_input_option="USER_ENTERED")
    _invalidate_tasks_cache()
    return task


def get_task_by_assignee(chat_id) -> Optional[Dict[str, Any]]:
    chat_id_str = str(chat_id)
    tasks = _read_all_tasks()
    active = [t for t in tasks if t.get("assignee_chat_id") == chat_id_str and t.get("status") in ACTIVE_STATUSES]
    if not active:
        return None
    active.sort(key=lambda t: t.get("created_at", ""))
    return active[0]


def has_active_task_for_assignee(chat_id) -> bool:
    chat_id_str = str(chat_id)
    for t in _read_all_tasks():
        if t.get("assignee_chat_id") == chat_id_str and t.get("status") in ACTIVE_STATUSES:
            return True
    return False


def get_oldest_queued_task(chat_id) -> Optional[Dict[str, Any]]:
    chat_id_str = str(chat_id)
    queued = [
        t for t in _read_all_tasks()
        if t.get("assignee_chat_id") == chat_id_str and t.get("status") == "queued"
    ]
    if not queued:
        return None
    queued.sort(key=lambda t: t.get("created_at", ""))
    return queued[0]


def get_task_by_id(task_id: str) -> Optional[Dict[str, Any]]:
    for t in _read_all_tasks():
        if t.get("task_id") == task_id:
            return t
    return None


def is_task_active(chat_id) -> bool:
    return get_task_by_assignee(chat_id) is not None


def update_task_fields(task_id: str, updates: Dict[str, Any]) -> None:
    ws = _tab("Tasks")
    if ws is None:
        return
    idx = _find_task_row_index(ws, task_id)
    if idx < 0:
        return

    headers = ws.row_values(1)
    updates = dict(updates)
    updates.setdefault("updated_at", now_ts())

    for key, val in updates.items():
        if key in headers:
            col = headers.index(key) + 1
            ws.update_cell(idx, col, str(val) if val is not None else "")
    _invalidate_tasks_cache()


def append_task_history(task_id: str, role: str, text: str) -> None:
    ws = _tab("TaskHistory")
    if ws is None:
        return
    ts = now_ts()
    truncated = text[:40000]
    ws.append_row(
        [task_id, ts, role, truncated],
        value_input_option="USER_ENTERED",
    )

    # 캐시가 살아있다면 동일 task 엔트리에도 push (stale 방지)
    global _history_cache
    if _history_cache is not None:
        entry = {"task_id": task_id, "ts": ts, "role": role, "text": truncated}
        _history_cache.setdefault(task_id, []).append(entry)


def _load_history_cache(force: bool = False) -> Dict[str, List[Dict[str, Any]]]:
    global _history_cache, _history_cache_at
    now = time.time()
    if not force and _history_cache is not None and (now - _history_cache_at) < _HISTORY_CACHE_TTL:
        return _history_cache

    ws = _tab("TaskHistory")
    if ws is None:
        _history_cache = {}
        _history_cache_at = now
        return _history_cache

    values = ws.get_all_values()
    if not values:
        _history_cache = {}
        _history_cache_at = now
        return _history_cache

    headers = values[0]
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in values[1:]:
        if not row or not row[0]:
            continue
        d = _row_to_dict(headers, row)
        grouped.setdefault(row[0], []).append(d)

    _history_cache = grouped
    _history_cache_at = now
    return grouped


def get_task_history(task_id: str) -> List[Dict[str, Any]]:
    if not task_id:
        return []
    cache = _load_history_cache()
    return list(cache.get(task_id, []))


def get_overdue_tasks(no_reply_minutes: int, cooldown_minutes: int) -> List[Dict[str, Any]]:
    from datetime import datetime
    from app.util import KST

    tasks = _read_all_tasks()
    now = datetime.now(KST)
    overdue = []

    for t in tasks:
        if t.get("status") not in ACTIVE_STATUSES:
            continue
        updated_at = t.get("updated_at", "")
        try:
            dt = datetime.strptime(updated_at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
        except ValueError:
            continue
        diff_min = (now - dt).total_seconds() / 60
        if diff_min < no_reply_minutes:
            continue

        last_report = t.get("owner_reported_at", "")
        if last_report:
            try:
                last_dt = datetime.strptime(last_report, "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
                if (now - last_dt).total_seconds() / 60 < cooldown_minutes:
                    continue
            except ValueError:
                pass

        t["_diff_min"] = int(diff_min)
        overdue.append(t)

    return overdue


# =========================================================
# Similar past tasks (for learning)
# =========================================================
_TOKEN_RE = re.compile(r"[\w가-힣]+")


def _tokenize(text: str) -> List[str]:
    return [t.lower() for t in _TOKEN_RE.findall(text or "") if len(t) >= 2]


def find_similar_past_tasks(instruction: str, limit: int = 3) -> List[Dict[str, Any]]:
    if not instruction:
        return []
    target_tokens = set(_tokenize(instruction))
    if not target_tokens:
        return []

    tasks = _read_all_tasks()
    completed = [t for t in tasks if t.get("status") == "completed" and t.get("final_report")]

    scored: List[tuple] = []
    for t in completed:
        past_tokens = set(_tokenize(t.get("instruction", "")))
        if not past_tokens:
            continue
        overlap = len(target_tokens & past_tokens)
        if overlap == 0:
            continue
        score = overlap / max(1, len(target_tokens | past_tokens))
        scored.append((score, t))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [t for _, t in scored[:limit]]


# =========================================================
# News dedup (persistent across restarts)
# =========================================================
def count_active_tasks_for_assignee(chat_id) -> int:
    chat_id_str = str(chat_id)
    return sum(
        1 for t in _read_all_tasks()
        if t.get("assignee_chat_id") == chat_id_str and t.get("status") in ACTIVE_STATUSES
    )


def count_queued_tasks_for_assignee(chat_id) -> int:
    chat_id_str = str(chat_id)
    return sum(
        1 for t in _read_all_tasks()
        if t.get("assignee_chat_id") == chat_id_str and t.get("status") == "queued"
    )


def is_news_slot_sent(slot_key: str) -> bool:
    ws = _tab("NewsDedup")
    if ws is None:
        return False
    try:
        col = ws.col_values(1)
    except Exception:
        logger.exception("is_news_slot_sent read failed | key=%s", slot_key)
        return False
    return slot_key in col[1:] if col else False


def mark_news_slot_sent(slot_key: str) -> None:
    ws = _tab("NewsDedup")
    if ws is None:
        return
    try:
        ws.append_row([slot_key, now_ts()], value_input_option="USER_ENTERED")
    except Exception:
        logger.exception("mark_news_slot_sent failed | key=%s", slot_key)


def prune_news_dedup(keep_prefixes: List[str]) -> None:
    """Keep only rows whose slot_key starts with any of the given prefixes (e.g. today's date).
    Called opportunistically to avoid unbounded growth."""
    ws = _tab("NewsDedup")
    if ws is None:
        return
    try:
        values = ws.get_all_values()
        if len(values) <= 1:
            return
        keep = [values[0]]
        for row in values[1:]:
            if row and any(row[0].endswith(p) or p in row[0] for p in keep_prefixes):
                keep.append(row)
        if len(keep) == len(values):
            return
        ws.clear()
        ws.append_rows(keep, value_input_option="USER_ENTERED")
    except Exception:
        logger.exception("prune_news_dedup failed")
