from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from typing import List, Optional

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
# Dedicated executor so generate_content can be enforced with a wall-clock timeout.
# The upstream call is not actually cancelled (SDK doesn't expose cancellation),
# but the webhook thread is freed so Telegram doesn't retry the update.
_timeout_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="gemini")


def is_available() -> bool:
    return bool(config.GEMINI_API_KEY) and genai is not None


def get_client():
    global _client
    if not is_available():
        return None
    if _client is None:
        _client = genai.Client(api_key=config.GEMINI_API_KEY)
        logger.info(
            "Gemini client initialized | primary=%s | fallback=%s",
            config.GEMINI_MODEL,
            config.GEMINI_FALLBACK_MODEL,
        )
    return _client


def get_types():
    return genai_types


def _models_to_try() -> List[str]:
    models = [config.GEMINI_MODEL]
    if config.GEMINI_FALLBACK_MODEL and config.GEMINI_FALLBACK_MODEL != config.GEMINI_MODEL:
        models.append(config.GEMINI_FALLBACK_MODEL)
    return models


def _is_retryable(exc: Exception) -> bool:
    s = str(exc)
    # Google returns 503/UNAVAILABLE or 429/RESOURCE_EXHAUSTED when the model
    # or region is overloaded. Preview models get throttled more often.
    return (
        "503" in s
        or "UNAVAILABLE" in s
        or "429" in s
        or "RESOURCE_EXHAUSTED" in s
        or "overloaded" in s.lower()
    )


def _generate(prompt: str, max_output_tokens: int, temperature: float, json_mode: bool) -> Optional[str]:
    client = get_client()
    if client is None or genai_types is None:
        return None

    cfg_kwargs = dict(
        temperature=temperature,
        max_output_tokens=max_output_tokens,
    )
    if json_mode:
        cfg_kwargs["response_mime_type"] = "application/json"

    timeout_s = max(1, config.GEMINI_TIMEOUT_SECONDS)

    last_err: Optional[Exception] = None
    for idx, model in enumerate(_models_to_try()):
        def _call(m=model):
            return client.models.generate_content(
                model=m,
                contents=prompt,
                config=genai_types.GenerateContentConfig(**cfg_kwargs),
            )

        try:
            future = _timeout_executor.submit(_call)
            resp = future.result(timeout=timeout_s)
            text = (getattr(resp, "text", "") or "").strip()
            if idx > 0:
                logger.info("Gemini fallback succeeded | model=%s", model)
            return text or None
        except FuturesTimeout:
            last_err = TimeoutError(f"gemini timeout {timeout_s}s on {model}")
            logger.warning("Gemini call timed out | model=%s | timeout=%ss", model, timeout_s)
            if idx + 1 < len(_models_to_try()):
                continue
            return None
        except Exception as e:
            last_err = e
            # 어떤 종류 에러든 fallback 모델 한 번 더 시도. 기존엔 _is_retryable 만 fallback
            # 트리거였는데, "404 model not found"(preview retire) 같은 비-retryable 에러가
            # 떨어지면 silent fail 되던 함정을 차단.
            if idx + 1 < len(_models_to_try()):
                tag = "retryable" if _is_retryable(e) else type(e).__name__
                logger.warning(
                    "Gemini primary model %s failed (%s); trying fallback",
                    model, tag,
                )
                continue
            logger.exception("Gemini call failed on model=%s", model)
            return None

    if last_err is not None:
        logger.error("All Gemini models exhausted | last=%s", last_err)
    return None


def generate_json(prompt: str, max_output_tokens: int = 1600, temperature: float = 0.1) -> Optional[str]:
    return _generate(prompt, max_output_tokens, temperature, json_mode=True)


def generate_text(prompt: str, max_output_tokens: int = 2048, temperature: float = 0.3) -> Optional[str]:
    return _generate(prompt, max_output_tokens, temperature, json_mode=False)
