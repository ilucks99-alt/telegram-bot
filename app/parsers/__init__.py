import json
import os
import re
from typing import Any, Dict

_PROMPTS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "prompts")


def load_prompt(name: str) -> str:
    path = os.path.join(_PROMPTS_DIR, name)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def render_prompt(name: str, **kwargs: Any) -> str:
    tmpl = load_prompt(name)
    for key, val in kwargs.items():
        if isinstance(val, (dict, list)):
            val_str = json.dumps(val, ensure_ascii=False, indent=2)
        else:
            val_str = str(val if val is not None else "")
        tmpl = tmpl.replace("{" + key + "}", val_str)
    return tmpl


def safe_json_parse(raw: str) -> Dict[str, Any]:
    text = (raw or "").strip()
    if not text:
        raise ValueError("empty JSON text")
    text = re.sub(r"^```json\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^```\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and start < end:
            return json.loads(text[start:end + 1])
        raise
