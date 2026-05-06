import json
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

try:
    from google.oauth2.service_account import Credentials as _SACredentials
except ImportError:
    _SACredentials = None

_client: Optional["genai.Client"] = None
_mode: str = ""  # "vertex" | "api_key" — 진단용
# Dedicated executor so generate_content can be enforced with a wall-clock timeout.
# The upstream call is not actually cancelled (SDK doesn't expose cancellation),
# but the webhook thread is freed so Telegram doesn't retry the update.
_timeout_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="gemini")


def _vertex_credentials():
    """GOOGLE_SA_JSON 에서 Vertex AI 용 자격증명 + project_id 추출.
    Sheets 와 동일한 SA 재사용 — Vertex AI User 역할만 추가돼있으면 됨."""
    if _SACredentials is None:
        return None, None
    raw = (config.GOOGLE_SA_JSON or "").strip()
    if not raw:
        return None, None
    try:
        sa_info = json.loads(raw)
    except Exception:
        logger.exception("vertex: failed to parse GOOGLE_SA_JSON")
        return None, None
    project_id = sa_info.get("project_id")
    if not project_id:
        logger.warning("vertex: project_id missing in GOOGLE_SA_JSON")
        return None, None
    try:
        creds = _SACredentials.from_service_account_info(
            sa_info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
    except Exception:
        logger.exception("vertex: credentials build failed")
        return None, None
    return creds, project_id


def is_available() -> bool:
    if genai is None:
        return False
    if config.USE_VERTEX_AI:
        creds, project_id = _vertex_credentials()
        return creds is not None and project_id is not None
    return bool(config.GEMINI_API_KEY)


def get_client():
    global _client, _mode
    if genai is None:
        return None
    if _client is not None:
        return _client

    if config.USE_VERTEX_AI:
        creds, project_id = _vertex_credentials()
        if creds is None or project_id is None:
            logger.error("vertex mode requested but credentials/project missing")
            return None
        try:
            _client = genai.Client(
                vertexai=True,
                project=project_id,
                location=config.VERTEX_LOCATION,
                credentials=creds,
            )
            _mode = "vertex"
            logger.info(
                "Gemini client initialized (Vertex AI) | project=%s | location=%s | primary=%s | fallback=%s",
                project_id,
                config.VERTEX_LOCATION,
                config.GEMINI_MODEL,
                config.GEMINI_FALLBACK_MODEL,
            )
            return _client
        except Exception:
            logger.exception("vertex client init failed")
            return None

    if not config.GEMINI_API_KEY:
        return None
    _client = genai.Client(api_key=config.GEMINI_API_KEY)
    _mode = "api_key"
    logger.info(
        "Gemini client initialized (Developer API) | primary=%s | fallback=%s",
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
