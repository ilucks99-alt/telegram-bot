import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Request

from app import config
from app.db_engine import InvestmentDB
from app.handlers.news import run_scheduled_news_report
from app.handlers.router import process_user_message
from app.handlers.task import check_and_report_overdue_tasks, check_due_date_reminders
from app.logger import get_logger, setup_logging
from app.services import sheets
from app.util import extract_message_context

setup_logging()
logger = get_logger(__name__)


_db: Optional[InvestmentDB] = None


def get_db() -> InvestmentDB:
    if _db is None:
        raise RuntimeError("DB not initialized")
    return _db


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _db
    if not config.TELEGRAM_TOKEN:
        logger.warning("TELEGRAM_TOKEN is empty — webhook will fail")

    if not os.path.exists(config.MAIN_DB_XLSX):
        logger.error("Main DB not found: %s", config.MAIN_DB_XLSX)
        raise FileNotFoundError(config.MAIN_DB_XLSX)

    _db = InvestmentDB(config.MAIN_DB_XLSX)
    logger.info("InvestmentDB loaded")

    # Init Google Sheets tabs + seed members from local JSON if exists
    try:
        seed = None
        legacy = os.path.join(os.path.dirname(os.path.dirname(__file__)), "team_members.json")
        if os.path.exists(legacy):
            import json
            with open(legacy, "r", encoding="utf-8") as f:
                seed = json.load(f)
        sheets.ensure_tabs_initialized(seed_members=seed)
    except Exception:
        logger.exception("Google Sheets init failed (continuing)")

    yield
    logger.info("Shutting down")


app = FastAPI(title="telegram-bot", lifespan=lifespan)


# =========================================================
# Health
# =========================================================
@app.get("/")
@app.get("/health")
async def health():
    return {"status": "ok", "service": "telegram-bot"}


# =========================================================
# Webhook
# =========================================================
@app.post("/webhook/{secret}")
async def webhook(secret: str, request: Request):
    if secret != config.TELEGRAM_WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="invalid secret")

    try:
        update = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid json")

    msg_ctx = extract_message_context(update)
    chat_id = msg_ctx.get("chat_id")
    text = msg_ctx.get("text") or ""
    document = msg_ctx.get("document")

    if chat_id is None or (not text and not document):
        return {"ok": True, "skipped": True}

    try:
        process_user_message(get_db(), chat_id, text, msg_ctx)
    except Exception:
        logger.exception("process_user_message failed")

    return {"ok": True}


# =========================================================
# Cron endpoints (GitHub Actions)
# =========================================================
def _check_cron_secret(authorization: Optional[str]) -> None:
    if not authorization:
        raise HTTPException(status_code=401, detail="missing auth")
    token = authorization.replace("Bearer ", "").strip()
    if token != config.CRON_SECRET:
        raise HTTPException(status_code=401, detail="invalid cron secret")


@app.post("/cron/news")
async def cron_news(authorization: Optional[str] = Header(None)):
    _check_cron_secret(authorization)
    status = run_scheduled_news_report(get_db(), config.OWNER_CHAT_ID)
    return {"ok": True, "status": status}


@app.post("/cron/task-check")
async def cron_task_check(authorization: Optional[str] = Header(None)):
    _check_cron_secret(authorization)
    overdue_count = check_and_report_overdue_tasks()
    reminder_count = check_due_date_reminders()
    return {"ok": True, "overdue": overdue_count, "reminders": reminder_count}


# =========================================================
# Admin: set webhook (manual trigger)
# =========================================================
@app.post("/admin/set-webhook")
async def admin_set_webhook(url: str, authorization: Optional[str] = Header(None)):
    _check_cron_secret(authorization)
    from app.services.telegram import set_webhook
    result = set_webhook(url)
    return {"ok": True, "result": result}
