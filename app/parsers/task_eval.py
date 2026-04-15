from typing import Any, Dict, List, Optional

from app.logger import get_logger
from app.parsers import render_prompt, safe_json_parse
from app.services import gemini

logger = get_logger(__name__)


_ROLE_LABELS = {
    "user": "팀원 답변",
    "assistant": "AI 메시지(이전 평가 결과 - 절대 팀원 답변으로 취급 금지)",
    "system": "시스템",
}


def _format_history(history: List[Dict[str, Any]]) -> str:
    if not history:
        return "(이전 대화 없음)"
    parts = []
    for item in history[-10:]:
        role = item.get("role", "")
        text = item.get("text", "")
        label = _ROLE_LABELS.get(role, role)
        parts.append(f"[{label}]\n{text}")
    return "\n\n".join(parts)


def _format_project_context(ctx: Optional[Dict[str, Any]]) -> str:
    if not ctx:
        return "(연관 없음)"
    parts = [f"Project_ID: {ctx.get('Project_ID')}"]
    for label, key in [
        ("자산명", "Asset_Name"),
        ("운용사", "Manager"),
        ("자산군", "Asset_Class"),
        ("지역", "Region"),
        ("전략", "Strategy"),
        ("섹터", "Sector"),
        ("Vintage", "Vintage"),
        ("만기년도", "Maturity_Year"),
        ("약정(억)", "Commitment"),
        ("콜금액(억)", "Called"),
        ("잔액(억)", "Outstanding"),
        ("NAV(억)", "NAV"),
        ("IRR", "IRR"),
    ]:
        val = ctx.get(key)
        if val is not None and val != "":
            parts.append(f"- {label}: {val}")
    return "\n".join(parts)


def _format_similar_tasks(similar: List[Dict[str, Any]]) -> str:
    if not similar:
        return "(유사 업무 없음)"
    parts = []
    for i, t in enumerate(similar, 1):
        parts.append(
            f"[과거 업무 {i}]\n"
            f"지시: {t.get('instruction', '')[:500]}\n"
            f"완료 보고: {t.get('final_report', '')[:800]}"
        )
    return "\n\n".join(parts)


def evaluate_response(
    instruction: str,
    history: List[Dict[str, Any]],
    latest_reply: str,
    project_context: Optional[Dict[str, Any]] = None,
    similar_past_tasks: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    if not gemini.is_available():
        raise RuntimeError("GEMINI_API_KEY가 설정되지 않았습니다.")

    prompt = render_prompt(
        "task_evaluator.txt",
        instruction=instruction,
        project_context=_format_project_context(project_context),
        similar_past_tasks=_format_similar_tasks(similar_past_tasks or []),
        history=_format_history(history),
        latest_reply=latest_reply,
    )

    raw = gemini.generate_json(prompt, max_output_tokens=2048, temperature=0.2)
    if not raw:
        raise RuntimeError("Gemini 응답이 비어 있습니다.")

    try:
        data = safe_json_parse(raw)
    except Exception:
        logger.exception("task evaluation JSON parse failed | raw=%s", raw[:500])
        raise

    result = (data.get("result") or "").strip()
    if result not in ("feedback", "complete"):
        raise RuntimeError(f"Gemini 응답 result 값 오류: {data}")

    data.setdefault("message_to_assignee", "")
    data.setdefault("message_to_owner", "")
    return data
