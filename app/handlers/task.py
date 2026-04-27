import os
import threading
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
from app.util import now_ts

logger = get_logger(__name__)

# 동일 task 에 대해 finalize 가 두 스레드에서 거의 동시에 호출되는 걸 프로세스 내에서 직렬화.
# (예: cron 두 개가 동시에 overdue 를 줍거나, 팀원이 같은 답변을 중복 전송한 경우)
# Sheets 가 트랜잭션을 지원하지 않아 재조회 가드만으로는 얇은 race window 를 못 막으므로 락을 덧붙인다.
_finalize_lock = threading.Lock()


# =========================================================
# /지시 파싱
# =========================================================
def _parse_task_command(payload: str) -> Optional[Dict[str, Any]]:
    """
    파서: '이름 | 업무내용 [| project=BS00001505]'
    첫 세그먼트(이름)와 둘째 세그먼트(업무내용)는 필수, 나머지는 key=value.
    """
    segments = [s.strip() for s in payload.split("|")]
    if len(segments) < 2:
        return None

    assignee_name = segments[0]
    instruction = segments[1]
    if not assignee_name or not instruction:
        return None

    project_id = ""
    for seg in segments[2:]:
        if "=" not in seg:
            continue
        k, v = seg.split("=", 1)
        if k.strip().lower() == "project":
            project_id = v.strip()

    return {
        "assignee_name": assignee_name,
        "instruction": instruction,
        "project_id": project_id,
    }


def handle_task_command(db: InvestmentDB, owner_chat_id, raw: str) -> None:
    payload = raw.replace("/지시", "", 1).strip()

    parsed = _parse_task_command(payload)
    if parsed is None:
        send_message(
            owner_chat_id,
            "형식: /지시 이름 | 업무내용 [| project=BS00001505]",
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

    queue_this = sheets.has_active_task_for_assignee(assignee_chat_id)
    initial_status = "queued" if queue_this else "waiting_for_reply"

    # 큐 상한 체크 — 상한 초과 시 생성 자체를 거부해 담당자가 밀리지 않게 한다
    if queue_this:
        current_queue = sheets.count_queued_tasks_for_assignee(assignee_chat_id)
        if current_queue >= config.TASK_QUEUE_MAX:
            send_message(
                owner_chat_id,
                f"⚠️ {parsed['assignee_name']} 님의 대기열이 이미 {current_queue}건입니다 "
                f"(상한 {config.TASK_QUEUE_MAX}건).\n"
                f"앞 업무가 완료된 뒤 다시 지시해주세요.",
            )
            return

    task_id = f"TASK-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    try:
        task = sheets.create_task(
            task_id=task_id,
            assignee_name=parsed["assignee_name"],
            assignee_chat_id=assignee_chat_id,
            owner_chat_id=owner_chat_id,
            instruction=parsed["instruction"],
            project_id=project_id,
            initial_status=initial_status,
        )
    except Exception:
        logger.exception("create_task failed")
        send_message(owner_chat_id, "업무 생성 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.")
        return

    sheets.append_task_history(task_id, "system", f"업무 지시됨: {parsed['instruction']}")

    if queue_this:
        # 새로 추가됐으므로 방금 생성된 task 도 count 에 포함됨
        try:
            queue_position = sheets.count_queued_tasks_for_assignee(assignee_chat_id)
        except Exception:
            logger.exception("count_queued_tasks_for_assignee failed")
            queue_position = 0
        try:
            send_message(
                owner_chat_id,
                f"[업무 대기열 등록]\n"
                f"- 업무번호: {task['task_id']}\n"
                f"- 담당자: {task['assignee_name']}\n"
                f"- 내용: {task['instruction']}\n"
                f"- 대기순번: {queue_position}번째 (진행 중 업무 뒤)\n"
                f"- 앞 업무가 종료되는 즉시 자동 발송됩니다.",
            )
        except Exception:
            logger.exception("queue owner notify failed")
        return

    _send_task_to_assignee(task, project_ctx)


def _send_task_to_assignee(task: Dict[str, Any], project_ctx: Optional[Dict[str, Any]]) -> None:
    extras = []
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


def _activate_next_queued_task(db: InvestmentDB, assignee_chat_id) -> None:
    next_task = sheets.get_oldest_queued_task(assignee_chat_id)
    if not next_task:
        return

    project_ctx = None
    project_id = next_task.get("project_id") or ""
    if project_id:
        try:
            project_ctx = db.project_context(project_id)
        except Exception:
            logger.exception("queued task project context lookup failed")

    try:
        sheets.update_task_fields(
            next_task["task_id"],
            {"status": "waiting_for_reply", "last_activity_at": now_ts()},
        )
        next_task["status"] = "waiting_for_reply"
        _send_task_to_assignee(next_task, project_ctx)
    except Exception:
        logger.exception("activate next queued task failed")


# =========================================================
# 답변 평가 공통
# =========================================================
def _collect_user_replies_text(history: List[Dict[str, Any]], max_chars: int = 4000) -> str:
    parts = []
    for idx, item in enumerate([h for h in history if h.get("role") == "user"], start=1):
        parts.append(f"[답변 {idx}]\n{item.get('text','')}")
    combined = "\n\n".join(parts)
    return combined[:max_chars]


def _finalize_task_completed(
    db: InvestmentDB,
    task: Dict[str, Any],
    owner_msg: str,
    assignee_msg: str,
) -> None:
    # 전역 락 + 재조회 가드 조합.
    # cron 이중 발사 / webhook 중복 전송 등으로 동일 task 의 finalize 가 두 번 호출돼도
    # 락을 잡은 쪽만 Sheets 상태를 업데이트하고 메시지를 보낸다. 두 번째는 status="completed"
    # 를 보고 조용히 빠져나온다.
    with _finalize_lock:
        current = sheets.get_task_by_id(task["task_id"])
        if current and current.get("status") == "completed":
            logger.info(
                "task %s already completed, skipping duplicate finalize",
                task["task_id"],
            )
            return

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
        _activate_next_queued_task(db, task["assignee_chat_id"])


def _finalize_due_to_feedback_limit(db: InvestmentDB, task: Dict[str, Any]) -> None:
    history = sheets.get_task_history(task["task_id"])
    combined = _collect_user_replies_text(history)
    owner_msg = (
        f"[업무 결과 보고 - 보완 미완료 종료]\n"
        f"- 담당자: {task['assignee_name']}\n"
        f"- 업무: {task['instruction']}\n\n"
        f"[전체 답변 내역]\n{combined[:4000]}"
    )
    _finalize_task_completed(
        db, task, owner_msg, "최대 피드백 횟수에 도달하여 현재 수준으로 보고가 완료되었습니다."
    )


def _process_eval_result(
    db: InvestmentDB, task: Dict[str, Any], result: Dict[str, Any]
) -> None:
    if result.get("result") == "feedback":
        feedback_text = (result.get("message_to_assignee") or "").strip() or "조금 더 구체적으로 보완해주세요."

        current_round = int(task.get("feedback_round", 0) or 0)
        if current_round >= config.MAX_TASK_FEEDBACK_ROUND:
            _finalize_due_to_feedback_limit(db, task)
            return

        next_round = current_round + 1
        sheets.update_task_fields(
            task["task_id"],
            {
                "status": "feedback_sent",
                "feedback_round": next_round,
            },
        )
        sheets.append_task_history(task["task_id"], "assistant", feedback_text)
        task["feedback_round"] = str(next_round)
        task["status"] = "feedback_sent"

        # 팀원이 보완 답을 안 주고 자리를 떠버리는 패턴 방지 — 명시적으로
        # "아직 보고 전" 임을 알리고, 마침 답변이 더 없다면 종결 표현(/cancel,
        # "이상입니다" 등)으로 끝낼 수 있는 우회로를 안내한다.
        is_last_round = next_round >= config.MAX_TASK_FEEDBACK_ROUND
        notice_lines = []
        if is_last_round:
            notice_lines.append(
                f"⚠️ 마지막 보완 요청입니다 (round {next_round}/{config.MAX_TASK_FEEDBACK_ROUND}). "
                "이번 답변 후에는 추가 보완 없이 보고가 마무리됩니다."
            )
        else:
            notice_lines.append(
                f"⚠️ 아직 보고 완료 전입니다 (보완 round {next_round}/{config.MAX_TASK_FEEDBACK_ROUND})."
            )
        notice_lines.append(
            f"답변이 {config.TASK_NO_REPLY_MINUTES}분 이상 없으면 현재 답변 내역으로 자동 마무리됩니다. "
            "더 보완할 게 없으면 \"이상입니다\" 또는 \"보고 부탁드립니다\" 라고 답변해 주세요."
        )

        send_message(
            task["assignee_chat_id"],
            f"{feedback_text}\n\n" + "\n".join(notice_lines),
        )
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
            db, task, owner_msg, "답변이 정리되어 보고 완료되었습니다."
        )
        return

    raise RuntimeError(f"알 수 없는 결과: {result}")


def _build_evaluation_inputs(db: InvestmentDB, task: Dict[str, Any]):
    # 최신 user 답변은 latest_reply 로 따로 전달되므로 Gemini 평가 성공 후에 history 에 append 한다.
    # (실패 시 찌꺼기가 남아 재시도 시 중복되거나, LLM 이 자기 피드백을 팀원 답변으로 오해하는 걸 방지)
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

    sheets.update_task_fields(task["task_id"], {"status": "reviewing", "last_activity_at": now_ts()})
    task["status"] = "reviewing"

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
        sheets.update_task_fields(task["task_id"], {"status": "waiting_for_reply"})
        return

    sheets.append_task_history(task["task_id"], "user", normalized)
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
        return

    sheets.update_task_fields(task["task_id"], {"status": "processing_file", "last_activity_at": now_ts()})

    local_path: Optional[str] = None
    content: Optional[str] = None
    try:
        local_path = download_telegram_file(file_id, file_name=file_name)

        extracted = extract_text_from_file(local_path)
        if not extracted or len(extracted.strip()) < 50:
            send_message(chat_id, "파일에서 텍스트를 충분히 추출하지 못했습니다. 텍스트로 다시 보내주세요.")
            sheets.update_task_fields(task["task_id"], {"status": "waiting_for_reply"})
            return

        content = f"[파일 제출]\n- 파일명: {file_name}\n\n{extracted[:12000]}"

        sheets.update_task_fields(task["task_id"], {"status": "reviewing"})
        task["status"] = "reviewing"

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

    sheets.append_task_history(task["task_id"], "user", content)
    _process_eval_result(db, task, result)


# =========================================================
# /cancel
# =========================================================
def handle_cancel_command(db: InvestmentDB, chat_id) -> None:
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

    _activate_next_queued_task(db, task["assignee_chat_id"])


# =========================================================
# Overdue (cron 호출)
# =========================================================
def check_and_report_overdue_tasks(db: InvestmentDB) -> int:
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
        status = task.get("status", "")
        has_replies = status in ("feedback_sent", "reviewing")

        if has_replies:
            # 답변이 있는 상태 → 기존 답변 기반으로 최종 보고 + 완료 처리
            _finalize_due_to_feedback_limit(db, task)
        else:
            # 답변 없음(waiting_for_reply 등) → 타임아웃 종료
            diff_min = task.get("_diff_min", 0)
            owner_msg = (
                f"⚠️ 미완료 업무 종료 알림\n"
                f"- 업무번호: {task['task_id']}\n"
                f"- 담당자: {task['assignee_name']}\n"
                f"- 업무: {task['instruction'][:100]}\n"
                f"- 마지막 업데이트: {task.get('updated_at','')}\n"
                f"- 경과시간: {diff_min}분\n\n"
                f"응답 없음으로 해당 업무 세션을 종료합니다."
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
                {"status": "closed_due_to_timeout", "closed_at": now_ts()},
            )
            _activate_next_queued_task(db, task["assignee_chat_id"])

        count += 1
    return count


# =========================================================
# /이력 조회
# =========================================================
def handle_task_history_command(owner_chat_id, raw: str) -> None:
    payload = raw.replace("/이력", "", 1).strip()

    if not payload:
        tasks = sheets._read_all_tasks()
        completed = [t for t in tasks if t.get("status") == "completed"]
        completed.sort(key=lambda t: t.get("closed_at", ""), reverse=True)
        if not completed:
            send_message(owner_chat_id, "완료된 업무가 없습니다. 사용법: /이력 TASK-20260415-103000")
            return
        lines = ["[최근 완료 업무 5건]"]
        for t in completed[:5]:
            lines.append(
                f"- {t.get('task_id','')} | {t.get('assignee_name','')} | "
                f"{(t.get('instruction') or '')[:50]}"
            )
        lines.append("")
        lines.append("조회: /이력 TASK-xxxxxxxx")
        send_message(owner_chat_id, "\n".join(lines))
        return

    task_id = payload.split()[0].strip()
    task = sheets.get_task_by_id(task_id)
    if not task:
        send_message(owner_chat_id, f"업무를 찾지 못했습니다: {task_id}")
        return

    history = sheets.get_task_history(task_id)

    header = (
        f"[업무 이력]\n"
        f"- 업무번호: {task.get('task_id','')}\n"
        f"- 담당자: {task.get('assignee_name','')}\n"
        f"- 상태: {task.get('status','')}\n"
        f"- 지시일시: {task.get('created_at','')}\n"
        f"- 종료일시: {task.get('closed_at','') or '-'}\n"
        f"- 지시내용: {task.get('instruction','')}"
    )

    role_label = {"system": "시스템", "user": "담당자", "assistant": "AI"}
    history_lines = []
    for h in history:
        role = role_label.get(h.get("role", ""), h.get("role", ""))
        ts = h.get("ts", "")
        text = (h.get("text") or "").strip().replace("\n", " ")
        history_lines.append(f"[{ts}] {role}: {text[:200]}")

    final_report = task.get("final_report", "")

    parts = [header]
    if history_lines:
        parts.append("\n[히스토리]\n" + "\n".join(history_lines))
    else:
        parts.append("\n[히스토리] (없음)")
    if final_report:
        parts.append("\n[최종 보고]\n" + final_report[:3000])

    send_long_message(owner_chat_id, "\n".join(parts))
