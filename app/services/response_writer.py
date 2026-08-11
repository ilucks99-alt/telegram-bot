"""Gemini-backed presentation layer for portfolio answers.

The database formatter remains the source of truth.  This module only changes
how that already-confirmed answer is presented, and returns the original answer
whenever Gemini is unavailable or its rewrite fails validation.
"""

import re

from app.logger import get_logger
from app.parsers import render_prompt
from app.services import gemini

logger = get_logger(__name__)

_PROJECT_ID_PATTERN = re.compile(r"BS\d{6,10}", re.IGNORECASE)
_NUMBER_PATTERN = re.compile(r"(?<![A-Za-z])[-+]?\d[\d,]*(?:\.\d+)?")


def _extract_project_ids(text: str) -> set[str]:
    return {match.upper() for match in _PROJECT_ID_PATTERN.findall(text or "")}


def _extract_numbers(text: str) -> set[str]:
    # Remove Project_IDs first so punctuation immediately after an identifier
    # cannot make the numeric suffix look like a newly invented value.
    without_project_ids = _PROJECT_ID_PATTERN.sub("", text or "")
    return set(_NUMBER_PATTERN.findall(without_project_ids))


def _is_safe_rewrite(factual_answer: str, candidate: str) -> bool:
    """Check the two facts that a presentation rewrite must never alter."""
    if _extract_project_ids(factual_answer) != _extract_project_ids(candidate):
        logger.warning("answer rewrite rejected: Project_ID set changed")
        return False

    original_numbers = _extract_numbers(factual_answer)
    rewritten_numbers = _extract_numbers(candidate)
    if not rewritten_numbers.issubset(original_numbers):
        logger.warning("answer rewrite rejected: new numeric value introduced")
        return False

    return True


def write_natural_answer(
    *,
    answer_kind: str,
    user_question: str,
    interpretation: str,
    factual_answer: str,
) -> str:
    """Return a natural rewrite, or the original deterministic answer on failure."""
    original = factual_answer or ""
    if not original or not gemini.is_available():
        return original

    try:
        prompt = render_prompt(
            "response_writer.txt",
            answer_kind=answer_kind,
            user_question=user_question,
            interpretation=interpretation,
            factual_answer=original,
        )
        rewritten = gemini.generate_text(
            prompt,
            max_output_tokens=3000,
            temperature=0.65,
        )
    except Exception:
        logger.exception("answer rewrite failed | kind=%s", answer_kind)
        return original

    candidate = (rewritten or "").strip()
    if not candidate:
        logger.warning("answer rewrite returned empty | kind=%s", answer_kind)
        return original

    maximum_length = max(12000, len(original) * 3)
    if len(candidate) > maximum_length:
        logger.warning("answer rewrite rejected: response too long | kind=%s", answer_kind)
        return original

    if not _is_safe_rewrite(original, candidate):
        logger.warning("answer rewrite rejected by safety check | kind=%s", answer_kind)
        return original

    return candidate
