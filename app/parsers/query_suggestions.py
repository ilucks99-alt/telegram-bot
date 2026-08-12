from typing import Any, Dict, List

from app.logger import get_logger
from app.parsers import render_prompt, safe_json_parse
from app.parsers.query import is_unprocessable_query, normalize_query_json
from app.services import gemini
from app.util import normalize_text

logger = get_logger(__name__)


def suggest_queries(
    user_question: str,
    query_json: Dict[str, Any],
    available_sectors: List[str],
) -> List[Dict[str, Any]]:
    if not gemini.is_available():
        return []
    prompt = render_prompt(
        "query_suggestions.txt",
        user_question=user_question,
        query_json=query_json,
        available_sectors=available_sectors,
    )
    raw = gemini.generate_json(prompt, max_output_tokens=1400, temperature=0.1)
    try:
        items = safe_json_parse(raw or "").get("suggestions", [])
    except Exception:
        logger.exception("query suggestion parse failed")
        return []
    result = []
    for item in items[:3]:
        if not isinstance(item, dict):
            continue
        label = str(item.get("label") or "").strip()[:35]
        normalized = normalize_query_json(item.get("query_json") or {})
        original_sectors = query_json.get("filters", {}).get("sector") or []
        suggested_sectors = normalized.get("filters", {}).get("sector") or []
        if original_sectors and not suggested_sectors:
            # A sector suggestion must name a real replacement. Removing the
            # condition would turn e.g. "rail" into all domestic infrastructure.
            continue
        if suggested_sectors != original_sectors:
            allowed = {normalize_text(value) for value in available_sectors}
            if not all(normalize_text(value) in allowed for value in suggested_sectors):
                continue
            label = f"'{', '.join(original_sectors)}' 대신 '{', '.join(suggested_sectors)}'로 찾을까요?"[:35]
        if label and not is_unprocessable_query(normalized) and normalized != query_json:
            result.append({"label": label, "query_json": normalized})
    return result
