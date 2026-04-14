from typing import Optional

from app import config
from app.logger import get_logger

logger = get_logger(__name__)

try:
    from google import genai
    from google.genai import types as genai_types
except ImportError:
    genai = None
    genai_types = None

_client: Optional["genai.Client"] = None


def is_available() -> bool:
    return bool(config.GEMINI_API_KEY) and genai is not None


def get_client():
    global _client
    if not is_available():
        return None
    if _client is None:
        _client = genai.Client(api_key=config.GEMINI_API_KEY)
        logger.info("Gemini client initialized | model=%s", config.GEMINI_MODEL)
    return _client


def get_types():
    return genai_types


def generate_json(prompt: str, max_output_tokens: int = 1600, temperature: float = 0.1) -> Optional[str]:
    client = get_client()
    if client is None or genai_types is None:
        return None
    try:
        resp = client.models.generate_content(
            model=config.GEMINI_MODEL,
            contents=prompt,
            config=genai_types.GenerateContentConfig(
                temperature=temperature,
                max_output_tokens=max_output_tokens,
                response_mime_type="application/json",
            ),
        )
        return (getattr(resp, "text", "") or "").strip() or None
    except Exception:
        logger.exception("Gemini JSON generation failed")
        return None


def generate_text(prompt: str, max_output_tokens: int = 2048, temperature: float = 0.3) -> Optional[str]:
    client = get_client()
    if client is None or genai_types is None:
        return None
    try:
        resp = client.models.generate_content(
            model=config.GEMINI_MODEL,
            contents=prompt,
            config=genai_types.GenerateContentConfig(
                temperature=temperature,
                max_output_tokens=max_output_tokens,
            ),
        )
        return (getattr(resp, "text", "") or "").strip() or None
    except Exception:
        logger.exception("Gemini text generation failed")
        return None
