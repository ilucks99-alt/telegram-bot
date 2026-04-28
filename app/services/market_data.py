"""Yahoo Finance 무료 chart API 에서 매크로 지표 스냅샷을 가져온다.

- API key 불필요. 클라우드 IP(Render 등) 차단을 피하기 위해 브라우저급 헤더 +
  세션 쿠키 + crumb 인증 + query1/query2 fallback 적용.
- 6개 지표를 ThreadPool 로 병렬 조회 (~1초 내 완료 기대).
- 개별 실패는 해당 지표만 스킵하고 나머지는 그대로 반환.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

import requests

from app.logger import get_logger

logger = get_logger(__name__)

_YF_HOSTS = ("query1.finance.yahoo.com", "query2.finance.yahoo.com")

# 봇 같은 UA(Mozilla/5.0 compatible; InvestBot/1.0)는 cloud IP 대역에서 401/빈응답으로
# 자주 떨어진다. 실제 Chrome UA + Accept-Language + Origin/Referer 셋이 가장 안정적.
_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9,ko;q=0.8",
    "Origin": "https://finance.yahoo.com",
    "Referer": "https://finance.yahoo.com/",
    "Connection": "keep-alive",
}

_session: Optional[requests.Session] = None
_crumb: Optional[str] = None
_session_lock = Lock()


def _build_session() -> Tuple[requests.Session, Optional[str]]:
    """Yahoo 쿠키(B) + crumb 을 미리 적재한 세션 생성. 실패해도 세션 자체는 반환."""
    s = requests.Session()
    s.headers.update(_BROWSER_HEADERS)
    crumb: Optional[str] = None
    # 1) fc.yahoo.com → B 쿠키 세팅 (401 응답이지만 Set-Cookie 가 박힘)
    try:
        s.get("https://fc.yahoo.com", timeout=5, allow_redirects=True)
    except Exception:
        pass
    # 2) finance.yahoo.com 메인 페이지 한번 들러서 추가 consent 쿠키 처리
    try:
        s.get("https://finance.yahoo.com/", timeout=5)
    except Exception:
        pass
    # 3) crumb 발급 (실패해도 chart 엔드포인트는 보통 통과 — 보험성).
    # 정상 crumb 은 짧은 영숫자+특수문자 문자열이라 공백·HTML 태그·과대 길이는 모두 reject.
    for host in _YF_HOSTS:
        try:
            r = s.get(f"https://{host}/v1/test/getcrumb", timeout=5)
        except Exception:
            continue
        if not r.ok:
            continue
        text = (r.text or "").strip()
        if not text or " " in text or "<" in text or len(text) > 64:
            continue
        crumb = text
        break
    return s, crumb


def _get_session() -> Tuple[requests.Session, Optional[str]]:
    global _session, _crumb
    if _session is not None:
        return _session, _crumb
    with _session_lock:
        if _session is None:
            _session, _crumb = _build_session()
        return _session, _crumb


def _reset_session() -> None:
    """403/401 등 권한 실패 시 다음 호출에서 세션을 재구축하도록 무효화."""
    global _session, _crumb
    with _session_lock:
        _session = None
        _crumb = None

# (symbol, Korean label, kind). kind="yield" 는 bp 로 diff 표기, 그 외는 %.
_INDICATORS_GLOBAL: List[Tuple[str, str, str]] = [
    ("KRW=X", "USD/KRW", "price"),
    ("^TNX", "US 10Y", "yield"),
    ("^VIX", "VIX", "price"),
    ("^GSPC", "S&P 500", "price"),
    ("GC=F", "Gold", "price"),
    ("CL=F", "WTI", "price"),
]

# 장 마감 후 보고용 — 국내 중심. 한국 채권금리는 Yahoo 가 직접 제공하지 않아
# 글로벌 영향 큰 US 10Y 만 유지하고 한국·일본 주가지수 + 환율로 구성.
_INDICATORS_DOMESTIC: List[Tuple[str, str, str]] = [
    ("^KS11", "KOSPI", "price"),
    ("^KQ11", "KOSDAQ", "price"),
    ("^KS200", "KOSPI 200", "price"),
    ("KRW=X", "USD/KRW", "price"),
    ("^N225", "Nikkei 225", "price"),
    ("^TNX", "US 10Y", "yield"),
]


def _parse_meta(data: Dict[str, Any]) -> Optional[Dict[str, float]]:
    result = (data.get("chart") or {}).get("result") or []
    if not result:
        return None
    meta = result[0].get("meta") or {}
    price = meta.get("regularMarketPrice")
    prev = meta.get("chartPreviousClose") or meta.get("previousClose")
    if price is None or prev is None:
        return None
    try:
        return {"price": float(price), "prev": float(prev)}
    except (TypeError, ValueError):
        return None


def _fetch_one(symbol: str, timeout: float = 6.0) -> Optional[Dict[str, float]]:
    """query1 → query2 fallback. 401/403 만나면 세션 재구축 후 1회 재시도.
    모든 시도 실패 시 None 을 반환해 호출자가 해당 지표를 스킵."""
    session, crumb = _get_session()
    params: Dict[str, str] = {"interval": "1d", "range": "5d"}
    if crumb:
        params["crumb"] = crumb

    auth_failed_once = False

    for host in _YF_HOSTS:
        url = f"https://{host}/v8/finance/chart/{symbol}"
        try:
            resp = session.get(url, params=params, timeout=timeout)
        except Exception:
            logger.warning("yahoo fetch error | host=%s symbol=%s", host, symbol, exc_info=False)
            continue

        if resp.status_code in (401, 403, 429):
            logger.warning(
                "yahoo auth/throttle | host=%s symbol=%s status=%s",
                host, symbol, resp.status_code,
            )
            if not auth_failed_once:
                # 세션이 만료/차단됐을 가능성 — 한 번만 재구축 후 재시도
                _reset_session()
                session, crumb = _get_session()
                if crumb:
                    params["crumb"] = crumb
                auth_failed_once = True
                try:
                    resp = session.get(url, params=params, timeout=timeout)
                except Exception:
                    continue
                if resp.status_code in (401, 403, 429):
                    continue
            else:
                continue

        if not resp.ok:
            logger.warning(
                "yahoo http error | host=%s symbol=%s status=%s",
                host, symbol, resp.status_code,
            )
            continue

        try:
            data = resp.json()
        except Exception:
            logger.warning("yahoo json parse failed | host=%s symbol=%s", host, symbol)
            continue

        parsed = _parse_meta(data)
        if parsed is not None:
            return parsed
        # 200 + 빈 result 는 다음 host 시도

    logger.warning("yahoo fetch exhausted | symbol=%s", symbol)
    return None


def _format_indicator(label: str, data: Dict[str, float], kind: str) -> str:
    price = data["price"]
    prev = data["prev"]
    diff = price - prev
    pct = (diff / prev * 100.0) if prev else 0.0
    arrow = "▲" if diff > 0 else ("▼" if diff < 0 else "─")

    if kind == "yield":
        bp = diff * 100.0  # yield 는 이미 % 단위, 1% = 100bp
        return f"- {label}: {price:.2f}% ({arrow}{abs(bp):.0f}bp)"
    return f"- {label}: {price:,.2f} ({arrow}{abs(pct):.2f}%)"


def _snapshot_entries(indicators: List[Tuple[str, str, str]]) -> List[str]:
    lines: List[Tuple[int, str]] = []
    with ThreadPoolExecutor(max_workers=len(indicators) or 1) as pool:
        futures = {
            pool.submit(_fetch_one, sym): (idx, sym, label, kind)
            for idx, (sym, label, kind) in enumerate(indicators)
        }
        for fut in as_completed(futures):
            idx, sym, label, kind = futures[fut]
            data = fut.result()
            if data is None:
                continue
            lines.append((idx, _format_indicator(label, data, kind)))

    lines.sort(key=lambda x: x[0])
    return [line for _, line in lines]


def build_macro_briefing(focus: str = "global") -> Optional[str]:
    """매크로 지표 블록을 빌드.
    focus="domestic" 이면 국내 중심(KOSPI/KOSDAQ 등), 그 외엔 글로벌 셋.
    하나도 못 가져오면 None 을 반환해 호출자가 스킵하게 한다."""
    indicators = _INDICATORS_DOMESTIC if focus == "domestic" else _INDICATORS_GLOBAL
    title = "📊 국내 매크로 (전일 종가 대비)" if focus == "domestic" else "📊 매크로 지표 (전일 종가 대비)"
    try:
        entries = _snapshot_entries(indicators)
    except Exception:
        logger.exception("macro briefing snapshot failed | focus=%s", focus)
        return None
    if not entries:
        return None
    return f"{title}\n" + "\n".join(entries)
