import json
import logging
import os
import re
from datetime import datetime

from google import genai

import config
from telegram_service import send_message, send_long_message, download_telegram_file
from file_extract_service import extract_text_from_file


# =========================================================
# 공통 유틸
# =========================================================
def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _safe_str(v) -> str:
    return "" if v is None else str(v)


def _truncate(text: str, max_len: int = 4000) -> str:
    text = (text or "").strip()
    if len(text) <= max_len:
        return text
    return text[:max_len] + "\n...(생략)"


# =========================================================
# 세션 저장/조회
# =========================================================
def load_task_sessions() -> dict:
    if not os.path.exists(config.TASK_SESSION_FILE):
        return {}

    with open(config.TASK_SESSION_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_task_sessions(data: dict) -> None:
    with open(config.TASK_SESSION_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_active_task_session(chat_id) -> dict | None:
    sessions = load_task_sessions()
    return sessions.get(str(chat_id))


def set_task_session(chat_id, session: dict) -> None:
    sessions = load_task_sessions()
    sessions[str(chat_id)] = session
    save_task_sessions(sessions)


def clear_task_session(chat_id) -> None:
    sessions = load_task_sessions()
    sessions.pop(str(chat_id), None)
    save_task_sessions(sessions)


def is_active_task_session(chat_id) -> bool:
    session = get_active_task_session(chat_id)
    if not session:
        return False
    return session.get("status") in ("waiting_for_reply", "feedback_sent", "processing_file", "reviewing")


# =========================================================
# 세션 생성/이력
# =========================================================
def create_task_session(
    owner_chat_id,
    assignee_chat_id,
    assignee_name: str,
    instruction: str,
    task_id: str | None = None,
) -> dict:
    if not task_id:
        task_id = f"TASK-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    session = {
        "task_id": task_id,
        "owner_chat_id": str(owner_chat_id),
        "assignee_chat_id": str(assignee_chat_id),
        "assignee_name": assignee_name,
        "instruction": instruction,
        "status": "waiting_for_reply",
        "feedback_round": 0,
        "max_feedback_round": config.MAX_TASK_FEEDBACK_ROUND,
        "history": [],
        "created_at": _now_str(),
        "updated_at": _now_str(),
        "last_activity_at": _now_str(),
    }

    set_task_session(assignee_chat_id, session)
    return session


def append_history(session: dict, role: str, text: str) -> dict:
    session["last_activity_at"] = _now_str()
    session["history"].append({
        "role": role,
        "text": text,
        "ts": _now_str(),
    })
    session["updated_at"] = _now_str()
    return session


def update_session_status(session: dict, chat_id, status: str) -> None:
    session["status"] = status
    session["updated_at"] = _now_str()
    set_task_session(chat_id, session)


# =========================================================
# 알림 관련
# =========================================================
def notify_owner_task_status(session: dict, stage: str, detail: str = "") -> None:
    try:
        msg = (
            f"[업무 진행 상태]\n"
            f"- 업무번호: {session['task_id']}\n"
            f"- 담당자: {session['assignee_name']}\n"
            f"- 상태: {stage}"
        )
        if detail:
            msg += f"\n- 상세: {detail}"
        #send_message(session["owner_chat_id"], msg)
    except Exception:
        logging.exception("notify_owner_task_status failed")


def notify_assignee_task_status(chat_id, stage: str, detail: str = "") -> None:
    try:
        msg = f"[처리 상태]\n- 상태: {stage}"
        if detail:
            msg += f"\n- 상세: {detail}"
        send_message(chat_id, msg)
    except Exception:
        logging.exception("notify_assignee_task_status failed")


def send_task_to_assignee(session: dict) -> None:
    msg = (
        f"[업무 지시]\n"
        f"- 업무번호: {session['task_id']}\n"
        f"- 내용: {session['instruction']}\n\n"
        f"답변을 텍스트로 작성하여 한번에 전송하여 주세요.\n"
        f"AI가 검토 후 필요 시 보완 요청을 드립니다."
    )
    send_message(session["assignee_chat_id"], msg)

    try:
        send_message(
            session["owner_chat_id"],
            f"[업무 발송 완료]\n"
            f"- 업무번호: {session['task_id']}\n"
            f"- 담당자: {session['assignee_name']}\n"
            f"- 내용: {session['instruction']}"
        )
    except Exception:
        logging.exception("send owner task dispatch message failed")


# =========================================================
# 히스토리 조합
# =========================================================
def _build_history_text(history: list[dict]) -> str:
    parts = []
    for item in history[-10:]:
        role = item.get("role", "")
        text = item.get("text", "")
        parts.append(f"[{role}] {text}")
    return "\n".join(parts)


def _collect_user_replies(history: list[dict]) -> list[str]:
    replies = []

    for item in history:
        if item.get("role") == "user":
            text = (item.get("text") or "").strip()
            if text:
                replies.append(text)

    return replies


def _build_combined_user_reply_text(session: dict, max_chars: int = 4000) -> str:
    replies = _collect_user_replies(session.get("history", []))

    if not replies:
        return ""

    parts = []
    for idx, reply in enumerate(replies, start=1):
        parts.append(f"[답변 {idx}]\n{reply}")

    combined = "\n\n".join(parts)
    return combined[:max_chars]


# =========================================================
# Gemini 응답 JSON 파싱 보강
# =========================================================
def _extract_json_object(raw: str) -> dict:
    text = (raw or "").strip()
    if not text:
        raise RuntimeError("Gemini 응답이 비어 있습니다.")

    # code fence 제거
    text = re.sub(r"^```json\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^```\s*", "", text)
    text = re.sub(r"\s*```$", "", text)

    # 1차: 전체 파싱
    try:
        return json.loads(text)
    except Exception:
        pass

    # 2차: 첫 { ~ 마지막 } 구간 파싱
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and start < end:
        candidate = text[start:end + 1]
        try:
            return json.loads(candidate)
        except Exception:
            pass

    raise RuntimeError(f"Gemini 응답 JSON 파싱 실패: {raw}")


# =========================================================
# Gemini 평가
# =========================================================
def evaluate_task_response_with_gemini(session: dict, latest_reply: str) -> dict:
    if not config.GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY가 설정되지 않았습니다.")

    client = genai.Client(api_key=config.GEMINI_API_KEY)
    history_text = _build_history_text(session.get("history", []))

    prompt = f"""
당신은 팀원의 업무 답변을 검토하는 실무형 리뷰어다.
대체투자 관련해서 많은 경험을 가지고 있는 전문가이다.
한국 및 글로벌 PE, VC, PD, 부동산, 인프라 등 대체 관련 모든 자산을 총괄하는 사업부장이라고 생각하고 리뷰를 작성해라.

[업무 지시]
{session['instruction']}

[대화 이력]
{history_text}

[이번 팀원 답변]
{latest_reply}

반드시 JSON 객체 1개만 출력하라.
설명문, 코드블록, 마크다운, ```json 같은 표시는 절대 쓰지 마라.

가능한 result 값은 "feedback" 또는 "complete" 뿐이다.

1) 답변이 충분하지 않으면:
{{
  "result": "feedback",
  "message_to_assignee": "아래 형식으로 작성\\n1. 첫 번째 보완 요청\\n2. 두 번째 보완 요청",
  "message_to_owner": ""
}}

2) 답변이 충분하면:
{{
  "result": "complete",
  "message_to_assignee": "",
  "message_to_owner": "[업무 결과 보고]\\n- 담당자: ...\\n- 업무: ...\\n\\n[핵심 내용]\\n1. ...\\n2. ...\\n3. ...\\n\\n[주요 리스크]\\n- ...\\n- ...\\n\\n[시사점]\\n- ..."
}}

판단 원칙:
- 불필요하게 피드백을 길게 하지 말 것
- 피드백은 최대 2개만 요청
- 답변이 대체로 충분하면 complete 처리
- 답변이 부족한 경우에는 부드럽고 상냥하게 feedback 할 것
- 팀원이 답변을 통해 보고 요청 시 complete 처리
- 보고문은 한국어로 구조화하여 작성할 것
- 핵심 내용에는 팀원의 답변이 충분히 표현되도록 할 것

"""

    resp = client.models.generate_content(
        model=config.GEMINI_MODEL,
        contents=prompt,
    )

    raw = (getattr(resp, "text", "") or "").strip()

    try:
        data = _extract_json_object(raw)
    except Exception:
        logging.exception("task evaluation json parse failed")
        raise

    result = (data.get("result") or "").strip()
    if result not in ("feedback", "complete"):
        raise RuntimeError(f"Gemini 응답 result 값이 올바르지 않습니다: {data}")

    data.setdefault("message_to_assignee", "")
    data.setdefault("message_to_owner", "")
    return data


# =========================================================
# 종료/보고 공통 처리
# =========================================================
def finalize_task_as_completed(chat_id, session: dict, owner_msg: str, assignee_msg: str) -> None:
    send_long_message(session["owner_chat_id"], owner_msg)
    session["status"] = "completed"
    set_task_session(chat_id, session)
    clear_task_session(chat_id)
    send_message(chat_id, assignee_msg)


def finalize_task_due_to_feedback_limit(chat_id, session: dict, title: str) -> None:
    combined_reply = _build_combined_user_reply_text(session)

    owner_msg = (
        f"[{title}]\n"
        f"- 담당자: {session['assignee_name']}\n"
        f"- 업무: {session['instruction']}\n\n"
        f"[전체 답변 내역]\n{combined_reply[:4000]}"
    )

    send_long_message(session["owner_chat_id"], owner_msg)
    session["status"] = "completed"
    set_task_session(chat_id, session)
    clear_task_session(chat_id)
    send_message(chat_id, "최대 피드백 횟수에 도달하여 현재 수준으로 보고가 완료되었습니다.")


# =========================================================
# 텍스트 답변 처리
# =========================================================
def handle_task_text_reply(chat_id, text: str) -> None:
    session = get_active_task_session(chat_id)
    if not session:
        send_message(chat_id, "진행 중인 업무 세션이 없습니다.")
        return

    normalized_text = (text or "").strip()
    if not normalized_text:
        send_message(chat_id, "내용이 비어 있습니다.")
        return

    append_history(session, "user", normalized_text)
    update_session_status(session, chat_id, "reviewing")

    notify_owner_task_status(session, "텍스트 답변 수신", _truncate(normalized_text, 300))
    notify_assignee_task_status(chat_id, "AI 검토 중", "제출한 텍스트 답변을 검토하고 있습니다.")

    latest_reply = normalized_text[:12000]

    try:
        result = evaluate_task_response_with_gemini(session, latest_reply)
    except Exception as e:
        logging.exception("handle_task_text_reply failed")

        send_message(chat_id, "답변 처리 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.")
        send_long_message(
            session["owner_chat_id"],
            f"[업무 답변 처리 오류]\n"
            f"- 담당자: {session['assignee_name']}\n"
            f"- 업무: {session['instruction']}\n"
            f"- 오류: {str(e)}\n\n"
            f"[최근 답변]\n{latest_reply[:3000]}"
        )

        update_session_status(session, chat_id, "feedback_sent")
        return

    if result.get("result") == "feedback":
        feedback_text = result.get("message_to_assignee", "").strip() or "조금 더 구체적으로 보완해주세요."

        if session["feedback_round"] >= session["max_feedback_round"]:
            finalize_task_due_to_feedback_limit(chat_id, session, "업무 결과 보고 - 보완 미완료 종료")
            return

        session["feedback_round"] += 1
        append_history(session, "assistant", feedback_text)
        update_session_status(session, chat_id, "feedback_sent")

        send_message(chat_id, feedback_text)
        notify_owner_task_status(session, "보완 요청 발송", _truncate(feedback_text, 500))
        return

    if result.get("result") == "complete":
        owner_msg = result.get("message_to_owner", "").strip()

        if not owner_msg:
            combined_reply = _build_combined_user_reply_text(session)
            owner_msg = (
                f"[업무 결과 보고]\n"
                f"- 담당자: {session['assignee_name']}\n"
                f"- 업무: {session['instruction']}\n\n"
                f"[전체 답변 내역]\n{combined_reply[:4000]}"
            )

        finalize_task_as_completed(
            chat_id,
            session,
            owner_msg,
            "답변이 정리되어 보고 완료되었습니다."
        )
        return

    raise RuntimeError(f"알 수 없는 결과: {result}")


# =========================================================
# 파일 답변 처리
# =========================================================
def handle_task_document_reply(chat_id, document: dict) -> None:
    session = get_active_task_session(chat_id)
    if not session:
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
        notify_owner_task_status(session, "파일 업로드 실패", f"{file_name} / 10MB 초과")
        return

    update_session_status(session, chat_id, "processing_file")

    notify_assignee_task_status(chat_id, "파일 접수", f"파일명: {file_name}")
    notify_owner_task_status(session, "파일 수신", f"{file_name} ({file_size} bytes)")

    try:
        notify_assignee_task_status(chat_id, "파일 다운로드 중", file_name)
        local_path = download_telegram_file(file_id, file_name=file_name)

        notify_assignee_task_status(chat_id, "텍스트 추출 중", file_name)
        notify_owner_task_status(session, "텍스트 추출 중", file_name)

        extracted_text = extract_text_from_file(local_path)

        if not extracted_text or len(extracted_text.strip()) < 50:
            notify_assignee_task_status(chat_id, "텍스트 추출 실패", "추출된 내용이 너무 짧습니다.")
            notify_owner_task_status(session, "텍스트 추출 실패", file_name)
            send_message(
                chat_id,
                "파일에서 텍스트를 충분히 추출하지 못했습니다. 텍스트로 다시 보내주시거나 다른 파일 형식으로 올려주세요."
            )
            update_session_status(session, chat_id, "waiting_for_reply")
            return

        extracted_preview = extracted_text.strip()
        notify_assignee_task_status(
            chat_id,
            "텍스트 추출 완료",
            f"추출 글자 수: {len(extracted_preview)}"
        )
        notify_owner_task_status(
            session,
            "텍스트 추출 완료",
            f"{file_name} / 추출 글자 수: {len(extracted_preview)}"
        )

        content = f"[파일 제출]\n- 파일명: {file_name}\n\n{extracted_text[:12000]}"
        append_history(session, "user", content)

        update_session_status(session, chat_id, "reviewing")
        notify_assignee_task_status(chat_id, "AI 검토 중", "제출한 파일 내용을 검토하고 있습니다.")
        notify_owner_task_status(session, "AI 검토 중", file_name)

        result = evaluate_task_response_with_gemini(session, content)

    except Exception as e:
        logging.exception("handle_task_document_reply failed")

        send_message(chat_id, "파일 처리 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.")
        send_long_message(
            session["owner_chat_id"],
            f"[파일 답변 처리 오류]\n"
            f"- 담당자: {session['assignee_name']}\n"
            f"- 업무: {session['instruction']}\n"
            f"- 파일명: {file_name}\n"
            f"- 오류: {str(e)}"
        )

        update_session_status(session, chat_id, "waiting_for_reply")
        return

    if result.get("result") == "feedback":
        feedback_text = result.get("message_to_assignee", "").strip() or "보고서 내용은 확인했으나, 조금 더 구체적인 보완이 필요합니다."

        if session["feedback_round"] >= session["max_feedback_round"]:
            finalize_task_due_to_feedback_limit(chat_id, session, "업무 결과 보고 - 파일 제출 기준 종료")
            return

        session["feedback_round"] += 1
        append_history(session, "assistant", feedback_text)
        update_session_status(session, chat_id, "feedback_sent")

        send_message(chat_id, feedback_text)
        notify_owner_task_status(session, "파일 검토 후 보완 요청", _truncate(feedback_text, 500))
        return

    if result.get("result") == "complete":
        owner_msg = result.get("message_to_owner", "").strip()

        if not owner_msg:
            combined_reply = _build_combined_user_reply_text(session)
            owner_msg = (
                f"[업무 결과 보고]\n"
                f"- 담당자: {session['assignee_name']}\n"
                f"- 업무: {session['instruction']}\n\n"
                f"[전체 제출 내역]\n{combined_reply[:4000]}"
            )

        finalize_task_as_completed(
            chat_id,
            session,
            owner_msg,
            "파일 내용이 정리되어 보고 완료되었습니다."
        )
        return

    raise RuntimeError(f"알 수 없는 결과: {result}")


# =========================================================
# 미응답 / 미완료 체크
# =========================================================
from datetime import datetime, timedelta


def _parse_time(ts: str) -> datetime:
    return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")


def get_overdue_task_sessions():
    sessions = load_task_sessions()
    now = datetime.now()

    overdue = []

    for chat_id, s in sessions.items():
        if s.get("status") not in ("waiting_for_reply", "feedback_sent", "reviewing"):
            continue

        updated_at = _parse_time(s.get("updated_at"))
        diff_min = (now - updated_at).total_seconds() / 60

        if diff_min >= config.TASK_NO_REPLY_MINUTES:
            last_report = s.get("owner_reported_at")

            if last_report:
                last_report_dt = _parse_time(last_report)
                cooldown = (now - last_report_dt).total_seconds() / 60
                if cooldown < config.TASK_REPORT_COOLDOWN_MINUTES:
                    continue

            overdue.append((chat_id, s, int(diff_min)))

    return overdue


def mark_owner_reported(chat_id, session):
    session["owner_reported_at"] = _now_str()
    set_task_session(chat_id, session)


def check_and_report_overdue_tasks():
    overdue_list = get_overdue_task_sessions()

    for chat_id, session, diff_min in overdue_list:
        owner_msg = (
            f"⚠️ 미완료 업무 종료 알림\n"
            f"- 업무번호: {session['task_id']}\n"
            f"- 담당자: {session['assignee_name']}\n"
            f"- 업무: {session['instruction'][:100]}\n"
            f"- 마지막 업데이트: {session['updated_at']}\n"
            f"- 경과시간: {diff_min}분\n"
            f"- 상태: {session['status']}\n\n"
            f"응답 지연으로 해당 업무 세션을 종료합니다."
        )

        assignee_msg = (
            f"[업무 종료]\n"
            f"- 업무번호: {session['task_id']}\n"
            f"- 사유: 일정 시간 동안 응답이 없어 업무 세션이 종료되었습니다.\n"
            f"- 필요 시 사업부장님께 다시 보고 후 재지시 받아주시기 바랍니다."
        )

        try:
            send_long_message(session["owner_chat_id"], owner_msg)
        except Exception:
            logging.exception("send overdue message to owner failed")

        try:
            send_message(chat_id, assignee_msg)
        except Exception:
            logging.exception("send overdue close message to assignee failed")

        session["status"] = "closed_due_to_timeout"
        session["closed_at"] = _now_str()
        session["close_reason"] = "no_reply_timeout"
        set_task_session(chat_id, session)
        clear_task_session(chat_id)