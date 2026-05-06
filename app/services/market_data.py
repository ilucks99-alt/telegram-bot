"""매크로 지표 스냅샷 — 다중 소스 폴백 체인.

Render 같은 cloud datacenter IP 에서는 Yahoo / Stooq 가 광범위 차단/throttle 되므로
cloud 친화적 소스를 1·2차로 두고 기존 소스는 3·4차 보험으로만 유지.

우선순위:
1) 네이버 금융 (api.stock.naver.com / polling.finance.naver.com) — key 불필요, KR/JP/US 지수 + VIX 다 커버
2) FRED (api.stlouisfed.org) — 무료 key 필요, US10Y / 환율 / 원자재 daily
3) Stooq (/q/l) — cloud IP 에서 막히지만 로컬·일부 환경에서는 동작
4) Yahoo (chart API) — cloud IP 에 광범위 429, 거의 실패 가정
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

import requests

from app import config
from app.logger import get_logger

logger = get_logger(__name__)

_YF_HOSTS = ("query1.finance.yahoo.com", "query2.finance.yahoo.com")

# Yahoo 심볼 → 네이버 국내 지수 코드 (polling.finance.naver.com)
_NAVER_DOMESTIC_MAP: Dict[str, str] = {
    "^KS11": "KOSPI",
    "^KQ11": "KOSDAQ",
    "^KS200": "KPI200",
}

# Yahoo 심볼 → 네이버 해외 지수 코드 (api.stock.naver.com/index/{code}/basic)
# 네이버는 Reuters 스타일 dot-prefix 코드를 씀 (.INX = S&P, .N225 = Nikkei 등).
_NAVER_WORLD_MAP: Dict[str, str] = {
    "^GSPC": ".INX",
    "^DJI": ".DJI",
    "^IXIC": ".IXIC",
    "^N225": ".N225",
    "^VIX": ".VIX",
}

# Yahoo 심볼 → 네이버 marketindex (category, reutersCode).
# api.stock.naver.com/marketindex/{category}/{reutersCode} — closePrice + fluctuations(signed).
# Render 에서 stooq/FRED 가 막히거나 느려서 환율·원자재는 네이버로 우선 우회.
_NAVER_MARKETINDEX_MAP: Dict[str, Tuple[str, str]] = {
    "KRW=X": ("exchange", "FX_USDKRW"),  # USD/KRW (하나은행 고시)
    "GC=F": ("metals", "GCcv1"),         # COMEX Gold (USD/oz)
    "CL=F": ("energy", "CLcv1"),         # NYMEX WTI (USD/bbl)
}

# Yahoo 심볼 → FRED series id. 네이버에 클린 매핑이 없는 미 국채 yield 만 FRED 로.
# 단일 요청이라 Render egress latency 영향 적게 받음.
_FRED_MAP: Dict[str, str] = {
    "^TNX": "DGS10",                # US 10Y Treasury (yield, %)
}

# Yahoo 심볼 → Stooq 심볼 (cloud IP 에서 막히지만 로컬 fallback 으로 유지)
_STOOQ_MAP: Dict[str, str] = {
    "^GSPC": "^spx",
    "KRW=X": "usdkrw",
    "GC=F": "xauusd",
    "CL=F": "cl.f",
    "^KS11": "^kospi",
    "^N225": "^nkx",
}

# 봇 같은 UA 는 cloud IP 대역에서 401/빈응답으로 자주 떨어진다. 실제 Chrome UA 유지.
_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9,ko;q=0.8",
    "Connection": "keep-alive",
}

_NAVER_HEADERS = {
    **_BROWSER_HEADERS,
    "Referer": "https://m.stock.naver.com/",
    "Origin": "https://m.stock.naver.com",
}

_YAHOO_HEADERS = {
    **_BROWSER_HEADERS,
    "Origin": "https://finance.yahoo.com",
    "Referer": "https://finance.yahoo.com/",
}

_session: Optional[requests.Session] = None
_crumb: Optional[str] = None
_session_lock = Lock()


def _build_session() -> Tuple[requests.Session, Optional[str]]:
    """Yahoo 쿠키(B) + crumb 적재 세션. 실패해도 세션 자체는 반환."""
    s = requests.Session()
    s.headers.update(_YAHOO_HEADERS)
    crumb: Optional[str] = None
    try:
        s.get("https://fc.yahoo.com", timeout=5, allow_redirects=True)
    except Exception:
        pass
    try:
        s.get("https://finance.yahoo.com/", timeout=5)
    except Exception:
        pass
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
    global _session, _crumb
    with _session_lock:
        _session = None
        _crumb = None


# (symbol, Korean label, kind). kind="yield" 는 bp 로 diff 표기, 그 외는 %.
# 글로벌 + 국내 통합 셋 — 매크로 보고는 항상 이 10개 기준.
_INDICATORS_ALL: List[Tuple[str, str, str]] = [
    ("KRW=X", "USD/KRW", "price"),
    ("^TNX", "US 10Y", "yield"),
    ("^VIX", "VIX", "price"),
    ("^GSPC", "S&P 500", "price"),
    ("GC=F", "Gold", "price"),
    ("CL=F", "WTI", "price"),
    ("^KS11", "KOSPI", "price"),
    ("^KQ11", "KOSDAQ", "price"),
    ("^KS200", "KOSPI 200", "price"),
    ("^N225", "Nikkei 225", "price"),
]


def _to_float(v: Any) -> Optional[float]:
    """네이버 응답은 '7,259.22' 콤마 박힌 문자열일 수 있어 정규화."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).replace(",", "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _fetch_naver_domestic(code: str, timeout: float = 5.0) -> Optional[Dict[str, float]]:
    """polling.finance.naver.com — KOSPI/KOSDAQ/KPI200. closePriceRaw + compareToPreviousClosePriceRaw."""
    url = f"https://polling.finance.naver.com/api/realtime/domestic/index/{code}"
    try:
        resp = requests.get(url, timeout=timeout, headers=_NAVER_HEADERS)
    except Exception:
        logger.warning("naver-domestic fetch error | code=%s", code, exc_info=False)
        return None
    if not resp.ok:
        logger.warning("naver-domestic http error | code=%s status=%s", code, resp.status_code)
        return None
    try:
        datas = (resp.json() or {}).get("datas") or []
    except Exception:
        return None
    if not datas:
        return None
    d = datas[0]
    price = _to_float(d.get("closePriceRaw") or d.get("closePrice"))
    diff = _to_float(d.get("compareToPreviousClosePriceRaw") or d.get("compareToPreviousClosePrice"))
    if price is None or diff is None:
        return None
    direction = ((d.get("compareToPreviousPrice") or {}).get("code") or "").strip()
    # code: '2' 상승, '5' 하락, 그외 보합
    signed_diff = -diff if direction == "5" else diff
    prev = price - signed_diff
    if prev <= 0:
        return None
    return {"price": price, "prev": prev}


def _fetch_naver_world(code: str, timeout: float = 5.0) -> Optional[Dict[str, float]]:
    """api.stock.naver.com/index/{code}/basic — .INX/.N225/.VIX 등."""
    url = f"https://api.stock.naver.com/index/{code}/basic"
    try:
        resp = requests.get(url, timeout=timeout, headers=_NAVER_HEADERS)
    except Exception:
        logger.warning("naver-world fetch error | code=%s", code, exc_info=False)
        return None
    if not resp.ok:
        logger.warning("naver-world http error | code=%s status=%s", code, resp.status_code)
        return None
    try:
        d = resp.json() or {}
    except Exception:
        return None
    price = _to_float(d.get("closePrice"))
    diff = _to_float(d.get("compareToPreviousClosePrice"))
    if price is None or diff is None:
        return None
    direction = ((d.get("compareToPreviousPrice") or {}).get("code") or "").strip()
    signed_diff = -diff if direction == "5" else diff
    prev = price - signed_diff
    if prev <= 0:
        return None
    return {"price": price, "prev": prev}


def _fetch_naver_marketindex(category: str, code: str, timeout: float = 5.0) -> Optional[Dict[str, float]]:
    """api.stock.naver.com/marketindex/{category}/{code} — 환율/원자재.
    closePrice (콤마 박힌 문자열) + fluctuations (signed float). prev = close - fluctuations."""
    url = f"https://api.stock.naver.com/marketindex/{category}/{code}"
    try:
        resp = requests.get(url, timeout=timeout, headers=_NAVER_HEADERS)
    except Exception as e:
        logger.warning(
            "naver-mi fetch error | category=%s code=%s | %s: %s",
            category, code, type(e).__name__, e,
        )
        return None
    if not resp.ok:
        logger.warning("naver-mi http error | category=%s code=%s status=%s", category, code, resp.status_code)
        return None
    try:
        d = resp.json() or {}
    except Exception:
        return None
    # FX_USDKRW 만 exchangeInfo 래퍼 형식, 나머지(metals/energy)는 평면.
    if category == "exchange":
        d = d.get("exchangeInfo") or d
    price = _to_float(d.get("closePrice"))
    fluct = _to_float(d.get("fluctuations"))  # 이미 signed (음수 가능)
    if price is None or fluct is None:
        return None
    prev = price - fluct
    if prev <= 0:
        return None
    return {"price": price, "prev": prev}


def _fetch_fred(series_id: str, timeout: float = 15.0) -> Optional[Dict[str, float]]:
    """FRED observations — 최근 2개 non-null 관측치로 close + prev close 산출.
    FRED 결측은 '.' 문자열로 옴. limit=10 으로 받아 결측 스킵."""
    api_key = (config.FRED_API_KEY or "").strip()
    if not api_key:
        return None
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": "10",
    }
    try:
        resp = requests.get(url, params=params, timeout=timeout, headers=_BROWSER_HEADERS)
    except Exception as e:
        logger.warning("fred fetch error | series=%s | %s: %s", series_id, type(e).__name__, e)
        return None
    if not resp.ok:
        logger.warning("fred http error | series=%s status=%s", series_id, resp.status_code)
        return None
    try:
        obs = (resp.json() or {}).get("observations") or []
    except Exception:
        return None
    values: List[float] = []
    for o in obs:
        v = _to_float(o.get("value"))
        if v is None:
            continue
        values.append(v)
        if len(values) >= 2:
            break
    if len(values) < 2:
        return None
    return {"price": values[0], "prev": values[1]}


def _fetch_stooq(stooq_symbol: str, timeout: float = 5.0) -> Optional[Dict[str, float]]:
    """Stooq 단일 quote — flag sd2cp = Symbol, Date, Close, Prev. cloud IP 에선 자주 막힘."""
    if not stooq_symbol:
        return None
    url = f"https://stooq.com/q/l/?s={stooq_symbol}&f=sd2cp&h&e=csv"
    try:
        resp = requests.get(url, timeout=timeout, headers=_BROWSER_HEADERS)
    except Exception as e:
        logger.warning("stooq fetch error | symbol=%s | %s: %s", stooq_symbol, type(e).__name__, e)
        return None
    if not resp.ok:
        logger.warning("stooq http error | symbol=%s status=%s", stooq_symbol, resp.status_code)
        return None
    body = (resp.text or "").strip()
    if not body or "apikey" in body.lower():
        return None
    lines = body.splitlines()
    if len(lines) < 2:
        return None
    parts = lines[-1].split(",")
    if len(parts) < 4:
        return None
    date = parts[1].strip()
    close_s = parts[2].strip()
    prev_s = parts[3].strip()
    if date == "N/D" or close_s in ("N/D", "") or prev_s in ("N/D", ""):
        return None
    try:
        return {"price": float(close_s), "prev": float(prev_s)}
    except ValueError:
        return None


def _parse_yahoo_meta(data: Dict[str, Any]) -> Optional[Dict[str, float]]:
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


def _fetch_yahoo(symbol: str, timeout: float = 6.0) -> Optional[Dict[str, float]]:
    """query1 → query2. 401/403/429 시 1회만 세션 재구축. cloud IP 에선 거의 실패 예상."""
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
            logger.warning("yahoo http error | host=%s symbol=%s status=%s", host, symbol, resp.status_code)
            continue

        try:
            data = resp.json()
        except Exception:
            logger.warning("yahoo json parse failed | host=%s symbol=%s", host, symbol)
            continue

        parsed = _parse_yahoo_meta(data)
        if parsed is not None:
            return parsed

    logger.warning("yahoo fetch exhausted | symbol=%s", symbol)
    return None


def _fetch_one(symbol: str, timeout: float = 6.0) -> Optional[Dict[str, float]]:
    """소스 폴백 체인: 네이버 → FRED → Stooq → Yahoo. 첫 성공에서 종료."""
    # 1) 네이버 (cloud-friendly, key 불필요)
    naver_kr = _NAVER_DOMESTIC_MAP.get(symbol)
    if naver_kr:
        result = _fetch_naver_domestic(naver_kr, timeout=min(timeout, 5.0))
        if result is not None:
            return result
    naver_world = _NAVER_WORLD_MAP.get(symbol)
    if naver_world:
        result = _fetch_naver_world(naver_world, timeout=min(timeout, 5.0))
        if result is not None:
            return result
    naver_mi = _NAVER_MARKETINDEX_MAP.get(symbol)
    if naver_mi:
        cat, code = naver_mi
        result = _fetch_naver_marketindex(cat, code, timeout=min(timeout, 5.0))
        if result is not None:
            return result

    # 2) FRED — 네이버에 매핑 없는 US10Y 단일 케이스만. Render egress latency 흡수용 15s.
    fred_series = _FRED_MAP.get(symbol)
    if fred_series:
        result = _fetch_fred(fred_series, timeout=15.0)
        if result is not None:
            return result

    # 3) Stooq (cloud IP 에선 거의 실패, 로컬 보험)
    stooq_sym = _STOOQ_MAP.get(symbol)
    if stooq_sym:
        result = _fetch_stooq(stooq_sym, timeout=min(timeout, 5.0))
        if result is not None:
            return result

    # 4) Yahoo (마지막 수단)
    return _fetch_yahoo(symbol, timeout=timeout)


def _format_indicator(label: str, data: Dict[str, float], kind: str) -> str:
    price = data["price"]
    prev = data["prev"]
    diff = price - prev
    pct = (diff / prev * 100.0) if prev else 0.0
    arrow = "▲" if diff > 0 else ("▼" if diff < 0 else "─")

    if kind == "yield":
        bp = diff * 100.0  # yield 는 % 단위, 1% = 100bp
        return f"- {label}: {price:.2f}% ({arrow}{abs(bp):.0f}bp)"
    return f"- {label}: {price:,.2f} ({arrow}{abs(pct):.2f}%)"


def _snapshot_entries(indicators: List[Tuple[str, str, str]]) -> Tuple[List[str], List[str]]:
    """병렬 fetch 후 (성공 라인, 누락 라벨) 반환. 누락 라벨은 원래 순서 유지."""
    lines: List[Tuple[int, str]] = []
    missing: List[Tuple[int, str]] = []
    with ThreadPoolExecutor(max_workers=len(indicators) or 1) as pool:
        futures = {
            pool.submit(_fetch_one, sym): (idx, sym, label, kind)
            for idx, (sym, label, kind) in enumerate(indicators)
        }
        for fut in as_completed(futures):
            idx, sym, label, kind = futures[fut]
            data = fut.result()
            if data is None:
                missing.append((idx, label))
                continue
            lines.append((idx, _format_indicator(label, data, kind)))

    lines.sort(key=lambda x: x[0])
    missing.sort(key=lambda x: x[0])
    return [line for _, line in lines], [label for _, label in missing]


def build_macro_briefing(focus: str = "all") -> Optional[str]:
    """매크로 지표 블록 빌드 (글로벌+국내 통합 10개).
    헤더에 N/M 표기, 누락 항목은 footer 에 명시. focus 인자는 호환용으로만 남김.
    하나도 못 가져오면 None 반환."""
    indicators = _INDICATORS_ALL
    title_base = "📊 매크로 지표 (전일 종가 대비)"
    try:
        entries, missing = _snapshot_entries(indicators)
    except Exception:
        logger.exception("macro briefing snapshot failed | focus=%s", focus)
        return None
    if not entries:
        return None
    header = f"{title_base} — {len(entries)}/{len(indicators)}"
    body = "\n".join(entries)
    if missing:
        return f"{header}\n{body}\n⚠ 누락: {', '.join(missing)}"
    return f"{header}\n{body}"
