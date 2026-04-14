import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from app import config
from app.db_engine import InvestmentDB
from app.logger import get_logger
from app.parsers.task_eval import evaluate_response
from app.services import sheets
from app.services.file_extract import extract_text_from_file
from app.services.telegram import (
    download_telegram_file,
    send_long_message,
    send_message,
)
from app.util import now_ts, parse_due_at

logger = get_logger(__name__)


# =========================================================
# /지시 파싱
# =========================================================
_KV_KEYS = {"priority", "due", "project"}


def _parse_task_command(payload: str) -> Optional[Dict[str, Any]]:
    """
    파서: '이름 | 업무내용 [| priority=high] [| due=2026-04-20 10:00] [| project=BS00001505]'
    첫 세그먼트(이름)와 둘째 세그먼트(업무내용)는 필수, 나머지는 key=value.
    """
    segments = [s.strip() for s in payload.split("|")]
    if len(segments) < 2:
        return None

    assignee_name = segments[0]
    instruction = segments[1]
    if not assignee_name or not instruction:
        return None

    priority = "normal"
    due_raw = ""
    project_id = ""

    for seg in segments[2:]:
        if "=" not in seg:
            continue
        k, v = seg.split("=", 1)
        k = k.strip().lower()
        v = v.strip()
        if k == "priority" and v.lower() in ("high", "normal", "low"):
            priority = v.lower()
        elif k == "due":
            due_raw = v
        elif k == "project":
            project_id = v

    return {
        "assignee_name": assignee_name,
        "instruction": instruction,
        "priority": priority,
        "due_raw": due_raw,
        "project_id": project_id,
    }


def handle_task_command(db: InvestmentDB, owner_chat_id, raw: str) -> None:
    payload = raw.replace("/지시", "", 1).strip()

    parsed = _parse_task_command(payload)
    if parsed is None:
        send_message(
            owner_chat_id,
            "형식: /지시 이름 | 업무내용 [| priority=high|normal|low] [| due=2026-04-20 10:00] [| project=BS00001505]",
        )
        return

    assignee_chat_id = sheets.find_member_chat_id(parsed["assignee_name"])
    if not assignee_chat_id:
        send_message(
            owner_chat_id,
            f"담당자 등록 정보를 찾지 못했습니다: {parsed['assignee_name']}\n"
            f"먼저 해당 팀원이 /등록 이름 으로 등록해야 합니다.",
        )
        return

    due_at_str = ""
    if parsed["due_raw"]:
        due_dt = parse_due_at(parsed["due_raw"])
        if due_dt:
            due_at_str = due_dt.strftime("%Y-%m-%d %H:%M")
        else:
            send_message(
                owner_chat_id,
                f"⚠️ due 형식을 인식하지 못했습니다: {parsed['due_raw']} (YYYY-MM-DD HH:MM). 데드라인 없이 진행합니다.",
            )

    project_id = parsed["project_id"]
    project_ctx = None
    if project_id:
        project_ctx = db.project_context(project_id)
        if project_ctx is None:
            send_message(
                owner_chat_id,
                f"⚠️ Project_ID {project_id}를 DB에서 찾지 못했습니다. project 연결 없이 진행합니다.",
            )
            project_id = ""

    task_id = f"TASK-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    try:
        task = sheets.create_task(
            task_id=task_id,
            assignee_name=parsed["assignee_name"],
            assignee_chat_id=assignee_chat_id,
            owner_chat_id=owner_chat_id,
            instruction=parsed["instruction"],
            priority=parsed["priority"],
            due_at=due_at_str,
            project_id=project_id,
        )
    except Exception:
        logger.exception("create_task failed")
        send_message(owner_chat_id, "업무 생성 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.")
        return

    sheets.append_task_history(task_id, "system", f"업무 지시됨: {parsed['instruction']}")

    _send_task_to_assignee(task, project_ctx)


def _send_task_to_assignee(task: Dict[str, Any], project_ctx: Optional[Dict[str, Any]]) -> None:
    priority_badge = {"high": "🔴 긴급", "normal": "🟡 보통", "low": "🟢 낮음"}.get(task["priority"], "")
    extras = []
    if priority_badge:
        extras.append(f"우선순위: {priority_badge}")
    if task.get("due_at"):
        extras.append(f"데드라인: {task['due_at']}")
    if task.get("project_id"):
        line = f"연관 투자건: {task['project_id']}"
        if project_ctx and project_ctx.get("Asset_Name"):
            line += f" ({project_ctx['Asset_Name']})"
        extras.append(line)

    extras_text = ("\n" + "\n".join(extras)) if extras else ""

    msg = (
        f"[업무 지시]\n"
        f"- 업무번호: {task['task_id']}\n"
        f"- 내용: {task['instruction']}"
        f"{extras_text}\n\n"
        f"답변을 텍스트 또는 파일(PDF/DOCX/TXT)로 전송해 주세요.\n"
        f"AI가 검토 후 필요 시 보완 요청을 드립니다.\n"
        f"(중단: /cancel)"
    )
    send_message(task["assignee_chat_id"], msg)

    try:
        send_message(
            task["owner_chat_id"],
            f"[업무 발송 완료]\n"
            f"- 업무번호: {task['task_id']}\n"
            f"- 담당자: {task['assignee_name']}\n"
            f"- 내용: {task['instruction']}"
            f"{extras_text}",
        )
    except Exception:
        logger.exception("send owner task dispatch message failed")


# =========================================================
# 알림
# =========================================================
def _notify_owner(task: Dict[str, Any], stage: str, detail: str = "") -> None:
    if not config.NOTIFY_OWNER_STATUS_UPDATES:
        return
    try:
        msg = (
            f"[업무 진행 상태]\n"
            f"- 업무번호: {task['task_id']}\n"
            f"- 담당자: {task['assignee_name']}\n"
            f"- 상태: {stage}"
        )
        if detail:
            msg += f"\n- 상세: {detail}"
        send_message(task["owner_chat_id"], msg)
    except Exception:
        logger.exception("notify_owner failed")


def _notify_assignee(chat_id, stage: str, detail: str = "") -> None:
    try:
        msg = f"[처리 상태]\n- 상태: {stage}"
        if detail:
            msg += f"\n- 상세: {detail}"
        send_message(chat_id, msg)
    except Exception:
        logger.exception("notify_assignee failed")


# =========================================================
# 답변 평가 공통
# =========================================================
def _truncate(text: str, max_len: int = 4000) -> str:
    text = (text or "").strip()
    return text if len(text) <= max_len else text[:max_len] + "\n...(생략)"


def _collect_user_replies_text(history: List[Dict[str, Any]], max_chars: int = 4000) -> str:
    parts = []
    for idx, item in enumerate([h for h in history if h.get("role") == "user"], start=1):
        parts.append(f"[답변 {idx}]\n{item.get('text','')}")
    combined = "\n\n".join(parts)
    return combined[:max_chars]


def _finalize_task_completed(task: Dict[str, Any], owner_msg: str, assignee_msg: str) -> None:
    sheets.update_task_fields(
        task["task_id"],
        {
            "status": "completed",
            "closed_at": now_ts(),
            "final_report": owner_msg[:10000],
        },
    )
    sheets.append_task_history(task["task_id"], "assistant", owner_msg)
    send_long_message(task["owner_chat_id"], owner_msg)
    send_message(task["assignee_chat_id"], assignee_msg)


def _finalize_due_to_feedback_limit(task: Dict[str, Any]) -> None:
    history = sheets.get_task_history(task["task_id"])
    combined = _collect_user_replies_text(history)
    owner_msg = (
        f"[업무 결과 보고 - 보완 미완료 종료]\n"
        f"- 담당자: {task['assignee_name']}\n"
        f"- 업무: {task['instruction']}\n\n"
        f"[전체 답변 내역]\n{combined[:4000]}"
    )
    _finalize_task_completed(
        task, owner_msg, "최대 피드백 횟수에 도달하여 현재 수준으로 보고가 완료되었습니다."
    )


def _process_eval_result(
    db: InvestmentDB, task: Dict[str, Any], result: Dict[str, Any]
) -> None:
    if result.get("result") == "feedback":
        feedback_text = (result.get("message_to_assignee") or "").strip() or "조금 더 구체적으로 보완해주세요."

        current_round = int(task.get("feedback_round", 0) or 0)
        if current_round >= config.MAX_TASK_FEEDBACK_ROUND:
            _finalize_due_to_feedback_limit(task)
            return

        sheets.update_task_fields(
            task["task_id"],
            {
                "status": "feedback_sent",
                "feedback_round": current_round + 1,
            },
        )
        sheets.append_task_history(task["task_id"], "assistant", feedback_text)
        task["feedback_round"] = str(current_round + 1)
        task["status"] = "feedback_sent"

        send_message(task["assignee_chat_id"], feedback_text)
        _notify_owner(task, "보완 요청 발송", _truncate(feedback_text, 500))
        return

    if result.get("result") == "complete":
        owner_msg = (result.get("message_to_owner") or "").strip()
        if not owner_msg:
            history = sheets.get_task_history(task["task_id"])
            combined = _collect_user_replies_text(history)
            owner_msg = (
                f"[업무 결과 보고]\n"
                f"- 담당자: {task['assignee_name']}\n"
                f"- 업무: {task['instruction']}\n\n"
                f"[전체 답변 내역]\n{combined[:4000]}"
            )
        _finalize_task_completed(
            task, owner_msg, "답변이 정리되어 보고 완료되었습니다."
        )
        return

    raise RuntimeError(f"알 수 없는 결과: {result}")


def _build_evaluation_inputs(db: InvestmentDB, task: Dict[str, Any]):
    history = sheets.get_task_history(task["task_id"])

    project_ctx = None
    if task.get("project_id"):
        try:
            project_ctx = db.project_context(task["project_id"])
        except Exception:
            logger.exception("project context lookup failed")

    try:
        similar = sheets.find_similar_past_tasks(task["instruction"], limit=3)
    except Exception:
        logger.exception("similar past tasks lookup failed")
        similar = []

    return history, project_ctx, similar


# =========================================================
# 텍스트 답변
# =========================================================
def handle_task_text_reply(db: InvestmentDB, chat_id, text: str) -> None:
    task = sheets.get_task_by_assignee(chat_id)
    if not task:
        send_message(chat_id, "진행 중인 업무 세션이 없습니다.")
        return

    normalized = (text or "").strip()
    if not normalized:
        send_message(chat_id, "내용이 비어 있습니다.")
        return

    sheets.append_task_history(task["task_id"], "user", normalized)
    sheets.update_task_fields(task["task_id"], {"status": "reviewing", "last_activity_at": now_ts()})
    task["status"] = "reviewing"

    _notify_owner(task, "텍스트 답변 수신", _truncate(normalized, 300))
    _notify_assignee(chat_id, "AI 검토 중", "제출한 텍스트 답변을 검토하고 있습니다.")

    history, project_ctx, similar = _build_evaluation_inputs(db, task)
    latest = normalized[:12000]

    try:
        result = evaluate_response(
            instruction=task["instruction"],
            history=history,
            latest_reply=latest,
            project_context=project_ctx,
            similar_past_tasks=similar,
        )
    except Exception:
        logger.exception("handle_task_text_reply failed")
        send_message(chat_id, "답변 처리 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.")
        sheets.update_task_fields(task["task_id"], {"status": "feedback_sent"})
        return

    _process_eval_result(db, task, result)


# =========================================================
# 파일 답변
# =========================================================
def handle_task_document_reply(db: InvestmentDB, chat_id, document: Dict[str, Any]) -> None:
    task = sheets.get_task_by_assignee(chat_id)
    if not task:
        send_message(chat_id, "진행 중인 업무 세션이 없습니다.")
        return

    file_id = document.get("file_id")
    file_name = document.get("file_name") or "uploaded_file"
    file_size = document.get("file_size") or 0

    if not file_id:
        send_message(chat_id, "파일 정보가 올바르지 않습니다.")
        return

    if file_size > 10 * 1024 * 1024:
        send_message(chat_id, "파일이 너무 큽니다. 10MB 이하 파일로 올려주세요.")
        _notify_owner(task, "파일 업로드 실패", f"{file_name} / 10MB 초과")
        return

    sheets.update_task_fields(task["task_id"], {"status": "processing_file", "last_activity_at": now_ts()})
    _notify_assignee(chat_id, "파일 접수", f"파일명: {file_name}")
    _notify_owner(task, "파일 수신", f"{file_name} ({file_size} bytes)")

    local_path: Optional[str] = None
    try:
        _notify_assignee(chat_id, "파일 다운로드 중", file_name)
        local_path = download_telegram_file(file_id, file_name=file_name)

        _notify_assignee(chat_id, "텍스트 추출 중", file_name)
        _notify_owner(task, "텍스트 추출 중", file_name)

        extracted = extract_text_from_file(local_path)
        if not extracted or len(extracted.strip()) < 50:
            _notify_assignee(chat_id, "텍스트 추출 실패", "추출된 내용이 너무 짧습니다.")
            _notify_owner(task, "텍스트 추출 실패", file_name)
            send_message(chat_id, "파일에서 텍스트를 충분히 추출하지 못했습니다. 텍스트로 다시 보내주세요.")
            sheets.update_task_fields(task["task_id"], {"status": "waiting_for_reply"})
            return

        _notify_assignee(chat_id, "텍스트 추출 완료", f"추출 글자 수: {len(extracted)}")
        _notify_owner(task, "텍스트 추출 완료", f"{file_name} / 추출 글자 수: {len(extracted)}")

        content = f"[파일 제출]\n- 파일명: {file_name}\n\n{extracted[:12000]}"
        sheets.append_task_history(task["task_id"], "user", content)

        sheets.update_task_fields(task["task_id"], {"status": "reviewing"})
        task["status"] = "reviewing"
        _notify_assignee(chat_id, "AI 검토 중", "제출한 파일 내용을 검토하고 있습니다.")
        _notify_owner(task, "AI 검토 중", file_name)

        history, project_ctx, similar = _build_evaluation_inputs(db, task)
        result = evaluate_response(
            instruction=task["instruction"],
            history=history,
            latest_reply=content,
            project_context=project_ctx,
            similar_past_tasks=similar,
        )
    except Exception:
        logger.exception("handle_task_document_reply failed")
        send_message(chat_id, "파일 처리 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.")
        sheets.update_task_fields(task["task_id"], {"status": "waiting_for_reply"})
        return
    finally:
        if local_path and os.path.exists(local_path):
            try:
                os.remove(local_path)
            except OSError:
                logger.warning("failed to cleanup temp file: %s", local_path)

    _process_eval_result(db, task, result)


# =========================================================
# /cancel
# =========================================================
def handle_cancel_command(chat_id) -> None:
    task = sheets.get_task_by_assignee(chat_id)
    if not task:
        send_message(chat_id, "진행 중인 업무 세션이 없습니다.")
        return

    sheets.update_task_fields(
        task["task_id"],
        {
            "status": "cancelled_by_assignee",
            "closed_at": now_ts(),
        },
    )
    sheets.append_task_history(task["task_id"], "system", "팀원이 /cancel 로 세션 종료")

    send_message(chat_id, f"업무 세션 {task['task_id']}을 종료했습니다.")
    try:
        send_message(
            task["owner_chat_id"],
            f"[업무 취소 알림]\n"
            f"- 업무번호: {task['task_id']}\n"
            f"- 담당자: {task['assignee_name']}\n"
            f"- 업무: {task['instruction'][:200]}\n"
            f"- 사유: 담당자가 /cancel 로 종료",
        )
    except Exception:
        logger.exception("cancel owner notify failed")


# =========================================================
# Overdue / due reminder (cron 호출)
# =========================================================
def check_and_report_overdue_tasks() -> int:
    try:
        overdue_list = sheets.get_overdue_tasks(
            no_reply_minutes=config.TASK_NO_REPLY_MINUTES,
            cooldown_minutes=config.TASK_REPORT_COOLDOWN_MINUTES,
        )
    except Exception:
        logger.exception("get_overdue_tasks failed")
        return 0

    count = 0
    for task in overdue_list:
        diff_min = task.get("_diff_min", 0)
        owner_msg = (
            f"⚠️ 미완료 업무 종료 알림\n"
            f"- 업무번호: {task['task_id']}\n"
            f"- 담당자: {task['assignee_name']}\n"
            f"- 업무: {task['instruction'][:100]}\n"
            f"- 마지막 업데이트: {task.get('updated_at','')}\n"
            f"- 경과시간: {diff_min}분\n"
            f"- 상태: {task.get('status','')}\n\n"
            f"응답 지연으로 해당 업무 세션을 종료합니다."
        )
        assignee_msg = (
            f"[업무 종료]\n"
            f"- 업무번호: {task['task_id']}\n"
            f"- 사유: 일정 시간 동안 응답이 없어 업무 세션이 종료되었습니다.\n"
            f"- 필요 시 사업부장님께 다시 보고 후 재지시 받아주시기 바랍니다."
        )

        try:
            send_long_message(task["owner_chat_id"], owner_msg)
        except Exception:
            logger.exception("overdue owner send failed")
        try:
            send_message(task["assignee_chat_id"], assignee_msg)
        except Exception:
            logger.exception("overdue assignee send failed")

        sheets.update_task_fields(
            task["task_id"],
            {
                "status": "closed_due_to_timeout",
                "closed_at": now_ts(),
            },
        )
        count += 1
    return count


def check_due_date_reminders() -> int:
    try:
        soon = sheets.get_tasks_due_soon(within_minutes=config.TASK_DUE_REMINDER_MINUTES)
    except Exception:
        logger.exception("get_tasks_due_soon failed")
        return 0

    for task in soon:
        try:
            send_message(
                task["assignee_chat_id"],
                f"⏰ 데드라인 임박\n"
                f"- 업무번호: {task['task_id']}\n"
                f"- 데드라인: {task.get('due_at')}\n"
                f"- 업무: {task['instruction'][:200]}\n"
                f"곧 제출 바랍니다.",
            )
            sheets.update_task_fields(task["task_id"], {"due_reminder_sent": now_ts()})
        except Exception:
            logger.exception("due reminder send failed")
    return len(soon)
