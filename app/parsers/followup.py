import json as _json
from typing import Any, Dict, Optional

from app.logger import get_logger
from app.parsers import render_prompt, safe_json_parse
from app.parsers.analysis import normalize_analysis_json
from app.parsers.query import normalize_query_json
from app.services import gemini

logger = get_logger(__name__)


def _normalize_lookthrough_payload(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    pid = str((payload or {}).get("project_id") or "").strip().upper()
    import re as _re
    if not _re.fullmatch(r"BS\d{6,10}", pid):
        return None
    return {"project_id": pid}


def _normalize_exposure_payload(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    mode = str((payload or {}).get("mode") or "holding").strip().lower()
    query = str((payload or {}).get("query") or "").strip()
    if not query:
        return None
    if mode not in ("counterparty", "holding"):
        mode = "holding"
    return {"mode": mode, "query": query}


def parse_followup(
    kind: str,
    previous_payload: Dict[str, Any],
    previous_summary: str,
    user_text: str,
    extras: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Returns dict: {"mode": "patch"|"new", "kind": "query"|"analysis"|"lookthrough"|"exposure", "payload": ...}
    None if Gemini unavailable or parse failed.
    """
    if not gemini.is_available():
        return None

    prompt = render_prompt(
        "followup.txt",
        kind=kind,
        previous_payload=previous_payload,
        previous_summary=previous_summary,
        user_text=user_text,
        extras_json=_json.dumps(extras or {}, ensure_ascii=False),
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
    elif kind_out == "lookthrough":
        payload = _normalize_lookthrough_payload(payload)
        if payload is None:
            return None
    elif kind_out == "exposure":
        payload = _normalize_exposure_payload(payload)
        if payload is None:
            return None
    else:
        return None

    if mode not in ("patch", "new"):
        mode = "new" if kind_out in ("lookthrough", "exposure") else "patch"

    return {"mode": mode, "kind": kind_out, "payload": payload}
