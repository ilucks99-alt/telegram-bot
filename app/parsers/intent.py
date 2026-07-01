import re
from typing import Any, Dict

from app.logger import get_logger
from app.parsers import render_prompt, safe_json_parse
from app.services import gemini

logger = get_logger(__name__)

_PID_ONLY_PAT = re.compile(r"^\s*BS\d{6,10}\s*$", re.IGNORECASE)
_PORTFOLIO_HINT_PAT = re.compile(
    r"(BS\d{6,10}|펀드|포트폴리오|운용사|자산|투자|약정|잔액|NAV|IRR|DPI|TVPI|"
    r"미국|유럽|아시아|한국|국내|해외|글로벌|부동산|인프라|PE|VC|PD|사모|대출|"
    r"조회|검색|목록|상위|하위|높은|낮은|큰|작은)",
    re.IGNORECASE,
)


def _heuristic_intent(text: str) -> Dict[str, Any]:
    s = (text or "").strip()
    lower = s.lower()
    if not s:
        return {"intent": "unknown", "confidence": 0.0, "reason": "빈 입력"}
    if any(k in lower for k in ("룩쓰루", "lookthrough", "look through", "하위자산", "보유자산", "보유종목", "드릴다운")):
        return {"intent": "lookthrough", "confidence": 0.95, "reason": "룩쓰루 키워드"}
    if any(k in s for k in ("상세조회", "상세", "자세히", "전체 데이터", "전체데이터")):
        return {"intent": "detail", "confidence": 0.95, "reason": "상세조회 키워드"}
    if any(k in s for k in ("비중", "평균", "합계", "총액", "집계", "그룹별", "분포", "비교", "몇 %", "몇%")):
        return {"intent": "analysis", "confidence": 0.85, "reason": "분석/집계 키워드"}
    if "별" in s and any(k in s for k in ("약정", "잔액", "NAV", "콜", "IRR", "DPI", "TVPI", "투자금액")):
        return {"intent": "analysis", "confidence": 0.85, "reason": "'~별' 집계 표현"}
    if _PID_ONLY_PAT.match(s):
        return {"intent": "query", "confidence": 0.9, "reason": "Project_ID 단독 입력"}
    if _PORTFOLIO_HINT_PAT.search(s):
        return {"intent": "query", "confidence": 0.7, "reason": "포트폴리오 조회 키워드"}
    return {"intent": "unknown", "confidence": 0.4, "reason": "포트폴리오 질의 단서 부족"}


def classify_portfolio_intent(user_text: str) -> Dict[str, Any]:
    fallback = _heuristic_intent(user_text)
    # 키워드로 명확히 잡히는 케이스는 LLM 호출 없이 라우팅한다.
    if fallback["confidence"] >= 0.85:
        return fallback

    if not gemini.is_available():
        logger.warning("intent classify: Gemini unavailable; using heuristic")
        return fallback

    prompt = render_prompt("intent_router.txt", user_text=user_text)
    raw = gemini.generate_json(prompt, max_output_tokens=200, temperature=0.0)
    if not raw:
        logger.warning("intent classify: Gemini returned empty; using heuristic")
        return fallback

    try:
        data = safe_json_parse(raw)
    except Exception:
        logger.exception("intent classify: JSON parse failed; using heuristic")
        return fallback

    intent = str(data.get("intent") or "").strip().lower()
    if intent not in {"query", "analysis", "detail", "lookthrough", "unknown"}:
        return fallback
    try:
        confidence = float(data.get("confidence", 0))
    except (TypeError, ValueError):
        confidence = 0.0
    if confidence < 0.45:
        return fallback
    return {
        "intent": intent,
        "confidence": max(0.0, min(confidence, 1.0)),
        "reason": str(data.get("reason") or "").strip(),
    }
