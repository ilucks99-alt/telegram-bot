from typing import Any, Dict, List

from app.logger import get_logger
from app.parsers import render_prompt, safe_json_parse
from app.parsers.query import is_unprocessable_query, normalize_query_json
from app.services import gemini

logger = get_logger(__name__)


def suggest_queries(user_question: str, query_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not gemini.is_available():
        return []
    prompt = render_prompt("query_suggestions.txt", user_question=user_question, query_json=query_json)
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
        if label and not is_unprocessable_query(normalized) and normalized != query_json:
            result.append({"label": label, "query_json": normalized})
    return result
