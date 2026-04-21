import os
import threading
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Query, Request

from app import config
from app.db_engine import InvestmentDB
from app.handlers.news import run_manager_news_report, run_scheduled_news_report
from app.handlers.router import process_user_message
from app.handlers.task import check_and_report_overdue_tasks
from app.logger import get_logger, setup_logging
from app.services import sheets
from app.util import extract_message_context

setup_logging()
logger = get_logger(__name__)


_db: Optional[InvestmentDB] = None

# GH Actions 와 cron-job.org 가 거의 동시에 /cron/tick* 을 때리는 경우 같은 overdue task 를
# 두 스레드가 집어 두 번 보고를 내보내는 문제를 막는다. 논블로킹 락이므로 이미 실행 중이면 스킵.
_tick_lock = threading.Lock()


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

    import threading

    def _worker():
        try:
            process_user_message(get_db(), chat_id, text, msg_ctx)
        except Exception:
            logger.exception("process_user_message failed")

    threading.Thread(target=_worker, daemon=True).start()
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


def _run_tick():
    """통합 tick 로직 — 업무체크(매번) + 뉴스(슬롯 매칭 시)."""
    # 두 cron 엔드포인트가 동시에 호출돼도 tick 본체는 한 번만 돈다.
    if not _tick_lock.acquire(blocking=False):
        logger.info("cron tick already running, skipping duplicate call")
        return
    try:
        db = get_db()
        chat_id = config.OWNER_CHAT_ID
        try:
            check_and_report_overdue_tasks(db)
        except Exception:
            logger.exception("cron tick: task-check failed")
        try:
            run_manager_news_report(db, chat_id)
        except Exception:
            logger.exception("cron tick: manager-news failed")
        try:
            run_scheduled_news_report(db, chat_id)
        except Exception:
            logger.exception("cron tick: macro-news failed")
    finally:
        _tick_lock.release()


@app.post("/cron/tick")
async def cron_tick(authorization: Optional[str] = Header(None)):
    """통합 cron 엔드포인트 (GitHub Actions — Bearer 인증)."""
    _check_cron_secret(authorization)
    import threading
    threading.Thread(target=_run_tick, daemon=True).start()
    return {"ok": True, "status": "scheduled"}


@app.get("/cron/tick/{secret}")
async def cron_tick_url(secret: str):
    """통합 cron 엔드포인트 (cron-job.org 등 — URL 시크릿)."""
    if secret != config.CRON_SECRET:
        raise HTTPException(status_code=401, detail="invalid secret")
    import threading
    threading.Thread(target=_run_tick, daemon=True).start()
    return {"ok": True, "status": "scheduled"}


# Legacy endpoints (kept for manual testing)
@app.post("/cron/news")
async def cron_news(authorization: Optional[str] = Header(None)):
    _check_cron_secret(authorization)
    import threading
    threading.Thread(
        target=lambda: run_scheduled_news_report(get_db(), config.OWNER_CHAT_ID),
        daemon=True,
    ).start()
    return {"ok": True, "status": "scheduled"}


@app.post("/cron/news-managers")
async def cron_news_managers(authorization: Optional[str] = Header(None)):
    _check_cron_secret(authorization)
    import threading
    threading.Thread(
        target=lambda: run_manager_news_report(get_db(), config.OWNER_CHAT_ID),
        daemon=True,
    ).start()
    return {"ok": True, "status": "scheduled"}


@app.post("/cron/task-check")
async def cron_task_check(authorization: Optional[str] = Header(None)):
    _check_cron_secret(authorization)
    import threading
    threading.Thread(
        target=lambda: check_and_report_overdue_tasks(get_db()),
        daemon=True,
    ).start()
    return {"ok": True, "status": "scheduled"}


# =========================================================
# Admin: set webhook (manual trigger)
# =========================================================
@app.post("/admin/set-webhook")
async def admin_set_webhook(
    url: str = Query(...),
    authorization: Optional[str] = Header(None),
):
    _check_cron_secret(authorization)
    from app.services.telegram import set_webhook
    try:
        result = set_webhook(url)
        return {"ok": True, "result": result}
    except Exception as e:
        logger.exception("set_webhook failed")
        return {"ok": False, "error": str(e)}


# =========================================================
# Admin: diagnostics
# =========================================================
@app.get("/admin/diag")
async def admin_diag(authorization: Optional[str] = Header(None)):
    _check_cron_secret(authorization)

    from app.services import gemini, sheets
    result: dict = {
        "ok": True,
        "diag_version": "v3-2026-04-14",
        "gemini_available": gemini.is_available(),
        "gemini_model": config.GEMINI_MODEL,
        "sheets_available": sheets.is_available(),
        "owner_chat_id": config.OWNER_CHAT_ID,
        "db_rows": len(get_db().df) if _db is not None else 0,
    }

    # Quick Gemini smoke test — bypass the wrapper so exceptions surface
    try:
        client = gemini.get_client()
        types = gemini.get_types()
        if client is None or types is None:
            result["gemini_test_ok"] = False
            result["gemini_test_error"] = "client or types is None"
        else:
            resp = client.models.generate_content(
                model=config.GEMINI_MODEL,
                contents='Return this exact JSON and nothing else: {"ok":true,"echo":"hi"}',
                config=types.GenerateContentConfig(
                    temperature=0.1,
                    max_output_tokens=2000,
                    response_mime_type="application/json",
                ),
            )
            txt = (getattr(resp, "text", "") or "").strip()
            result["gemini_test_raw"] = txt[:500]
            result["gemini_test_ok"] = bool(txt)

            # Surface finish_reason, prompt_feedback, usage
            try:
                candidates = getattr(resp, "candidates", None) or []
                if candidates:
                    c0 = candidates[0]
                    fr = getattr(c0, "finish_reason", None)
                    result["gemini_finish_reason"] = str(fr)
                    content = getattr(c0, "content", None)
                    parts = getattr(content, "parts", None) if content else None
                    if parts:
                        result["gemini_parts"] = [
                            (getattr(p, "text", None) or "")[:200] for p in parts
                        ]
                    else:
                        result["gemini_parts"] = []
                pf = getattr(resp, "prompt_feedback", None)
                if pf is not None:
                    result["gemini_prompt_feedback"] = str(pf)[:400]
                usage = getattr(resp, "usage_metadata", None)
                if usage is not None:
                    result["gemini_usage"] = str(usage)[:400]
            except Exception as inner:
                result["gemini_inspect_error"] = f"{type(inner).__name__}: {str(inner)[:300]}"
    except Exception as e:
        import traceback
        result["gemini_test_ok"] = False
        result["gemini_test_error"] = f"{type(e).__name__}: {str(e)[:600]}"
        result["gemini_test_tb_tail"] = traceback.format_exc()[-1500:]

    # Sheets smoke test
    try:
        ss = sheets.get_spreadsheet()
        if ss is not None:
            result["sheets_title"] = ss.title
            result["sheets_tabs"] = [w.title for w in ss.worksheets()]
    except Exception as e:
        result["sheets_error"] = f"{type(e).__name__}: {str(e)[:300]}"

    return result
