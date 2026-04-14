from typing import Any, Dict, Optional

from app.logger import get_logger
from app.parsers import render_prompt, safe_json_parse
from app.parsers.analysis import normalize_analysis_json
from app.parsers.query import normalize_query_json
from app.services import gemini

logger = get_logger(__name__)


def parse_followup(kind: str, previous_payload: Dict[str, Any], previous_summary: str, user_text: str) -> Optional[Dict[str, Any]]:
    """
    Returns dict with shape:
      {"mode": "patch"|"new", "kind": "query"|"analysis", "payload": normalized_payload}
    Or None if Gemini unavailable or parse failed.
    """
    if not gemini.is_available():
        return None

    prompt = render_prompt(
        "followup.txt",
        kind=kind,
        previous_payload=previous_payload,
        previous_summary=previous_summary,
        user_text=user_text,
    )
    raw = gemini.generate_json(prompt, max_output_tokens=1600, temperature=0.1)
    if not raw:
        return None

    try:
        data = safe_json_parse(raw)
    except Exception:
        logger.exception("followup JSON parse failed")
        return None

    mode = (data.get("mode") or "").strip().lower()
    kind_out = (data.get("kind") or kind).strip().lower()
    payload = data.get("payload") or {}

    if kind_out == "query":
        payload = normalize_query_json(payload)
    elif kind_out == "analysis":
        payload = normalize_analysis_json(payload)
    else:
        return None

    if mode not in ("patch", "new"):
        mode = "patch"

    return {"mode": mode, "kind": kind_out, "payload": payload}
