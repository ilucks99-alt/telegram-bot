"""Yahoo Finance 무료 chart API 에서 매크로 지표 스냅샷을 가져온다.

- API key 불필요 (User-Agent 만 설정).
- 6개 지표를 ThreadPool 로 병렬 조회 (~1초 내 완료 기대).
- 개별 실패는 해당 지표만 스킵하고 나머지는 그대로 반환.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import requests

from app.logger import get_logger

logger = get_logger(__name__)

_YF_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=5d"
_UA = "Mozilla/5.0 (compatible; InvestBot/1.0)"

# (symbol, Korean label, kind). kind="yield" 는 bp 로 diff 표기, 그 외는 %.
_DEFAULT_INDICATORS: List[Tuple[str, str, str]] = [
    ("KRW=X", "USD/KRW", "price"),
    ("^TNX", "US 10Y", "yield"),
    ("^VIX", "VIX", "price"),
    ("^GSPC", "S&P 500", "price"),
    ("GC=F", "Gold", "price"),
    ("CL=F", "WTI", "price"),
]


def _fetch_one(symbol: str, timeout: float = 6.0) -> Optional[Dict[str, float]]:
    try:
        resp = requests.get(
            _YF_URL.format(symbol=symbol),
            headers={"User-Agent": _UA, "Accept": "application/json"},
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        logger.warning("yahoo fetch failed | symbol=%s", symbol, exc_info=False)
        return None

    try:
        result = (data.get("chart") or {}).get("result") or []
        if not result:
            return None
        meta = result[0].get("meta") or {}
        price = meta.get("regularMarketPrice")
        prev = meta.get("chartPreviousClose") or meta.get("previousClose")
        if price is None or prev is None:
            return None
        return {"price": float(price), "prev": float(prev)}
    except Exception:
        logger.exception("yahoo parse failed | symbol=%s", symbol)
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


def _snapshot_entries() -> List[str]:
    lines: List[Tuple[int, str]] = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {
            pool.submit(_fetch_one, sym): (idx, sym, label, kind)
            for idx, (sym, label, kind) in enumerate(_DEFAULT_INDICATORS)
        }
        for fut in as_completed(futures):
            idx, sym, label, kind = futures[fut]
            data = fut.result()
            if data is None:
                continue
            lines.append((idx, _format_indicator(label, data, kind)))

    lines.sort(key=lambda x: x[0])
    return [line for _, line in lines]


def build_macro_briefing() -> Optional[str]:
    """매크로 지표 블록을 빌드. 하나도 못 가져오면 None 을 반환해 호출자가 스킵하게 한다."""
    try:
        entries = _snapshot_entries()
    except Exception:
        logger.exception("macro briefing snapshot failed")
        return None
    if not entries:
        return None
    return "📊 매크로 지표 (전일 종가 대비)\n" + "\n".join(entries)
