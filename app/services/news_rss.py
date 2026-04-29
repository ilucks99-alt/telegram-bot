import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_to_datetime
from threading import Lock
from typing import List, Optional, Tuple

import requests

from app.logger import get_logger
from app.util import KST

logger = get_logger(__name__)

# 봇 티 나는 짧은 UA 는 Render 등 cloud IP 대역에서 차단되거나 빈 RSS 로 응답하는
# 경우가 잦다. Yahoo 우회와 동일한 브라우저급 헤더 + 세션 쿠키 방식 사용.
_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.5",
    "Accept-Language": "en-US,en;q=0.9,ko;q=0.8",
    "Connection": "keep-alive",
}

_session: Optional[requests.Session] = None
_session_lock = Lock()


def _build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(_BROWSER_HEADERS)
    # consent 쿠키를 받기 위해 메인 페이지 한번 방문 (실패해도 본 호출은 진행)
    try:
        s.get("https://news.google.com/", timeout=5)
    except Exception:
        pass
    return s


def _get_session() -> requests.Session:
    global _session
    if _session is not None:
        return _session
    with _session_lock:
        if _session is None:
            _session = _build_session()
        return _session


def _reset_session() -> None:
    global _session
    with _session_lock:
        _session = None


def build_effective_query(query: str) -> str:
    q = query.strip()
    if any(w in q for w in ["오늘", "금일", "today"]):
        q = q.replace("오늘의", "").replace("오늘", "").replace("금일", "").strip()
        q = f"{q} when:1d"
    elif any(w in q.lower() for w in ["latest", "최근", "최신"]):
        q = f"{q} when:7d"
    return q.strip()


def _parse_pub_date(pub_date: str) -> Optional[datetime]:
    try:
        dt = parsedate_to_datetime(pub_date)
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        return dt
    except Exception:
        return None


def _normalize_title(title: str) -> str:
    return " ".join(title.lower().split())


def _fetch_rss(url: str, timeout: int = 20) -> Optional[str]:
    """단일 RSS URL 호출. 비정상(HTTP 비-200, HTML 응답) 시 None 반환."""
    session = _get_session()
    try:
        resp = session.get(url, timeout=timeout)
    except Exception:
        logger.warning("google news rss fetch error | url=%s", url, exc_info=False)
        return None

    if resp.status_code in (401, 403, 429):
        logger.warning(
            "google news rss auth/throttle | url=%s status=%s",
            url, resp.status_code,
        )
        # 세션 쿠키가 만료/차단됐을 수 있으니 한번만 재구축 후 재시도
        _reset_session()
        try:
            resp = _get_session().get(url, timeout=timeout)
        except Exception:
            return None
        if resp.status_code != 200:
            return None

    if resp.status_code != 200:
        logger.warning(
            "google news rss bad status | url=%s status=%s",
            url, resp.status_code,
        )
        return None

    body = resp.text or ""
    stripped = body.lstrip()
    # RSS 가 아니라 HTML/에러 페이지가 온 경우 (cloud IP 차단 시 흔함)
    if not stripped.startswith("<?xml") and not stripped.startswith("<rss"):
        logger.warning(
            "google news rss non-xml body | url=%s head=%s",
            url, stripped[:80].replace("\n", " "),
        )
        return None
    return body


def _parse_rss_items(xml_text: str, seen: set) -> List[dict]:
    """RSS XML 에서 item 들을 파싱. seen 셋에 dedup key 자동 추가."""
    out: List[dict] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        logger.warning("google news rss parse error")
        return out

    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub_date = (item.findtext("pubDate") or "").strip()
        source_elem = item.find("source")
        source = source_elem.text.strip() if source_elem is not None and source_elem.text else ""

        published_at = _parse_pub_date(pub_date)
        if not title or not published_at:
            continue

        dedup = (_normalize_title(title), source.lower().strip())
        if dedup in seen:
            continue
        seen.add(dedup)

        out.append({
            "title": title,
            "link": link,
            "pub_date": pub_date,
            "published_at": published_at,
            "source": source,
        })
    return out


def search_google_news_rss(query: str, limit: int = 20) -> List[dict]:
    effective_query = build_effective_query(query)
    encoded = urllib.parse.quote(effective_query)

    locale_urls: List[Tuple[str, str]] = [
        ("ko", f"https://news.google.com/rss/search?q={encoded}&hl=ko&gl=KR&ceid=KR:ko"),
        ("en", f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"),
    ]

    results: List[dict] = []
    seen: set = set()
    counts = {"ko": 0, "en": 0, "bing": 0}

    for locale, url in locale_urls:
        body = _fetch_rss(url)
        if not body:
            continue
        items = _parse_rss_items(body, seen)
        counts[locale] = len(items)
        results.extend(items)

    # Render 등 cloud IP 에서 Google News 가 503/HTML 로 막힐 때를 위한 fallback —
    # Bing News RSS 는 key 불필요·cloud IP 친화적이고 한국어/영어 둘 다 정상 응답.
    if not results:
        bing_url = f"https://www.bing.com/news/search?q={encoded}&format=RSS"
        body = _fetch_rss(bing_url)
        if body:
            items = _parse_rss_items(body, seen)
            counts["bing"] = len(items)
            results.extend(items)

    logger.info(
        "news rss | kw=%s google_ko=%d google_en=%d bing=%d total=%d",
        query, counts["ko"], counts["en"], counts["bing"], len(results),
    )

    results.sort(key=lambda x: x["published_at"], reverse=True)

    original = query.strip().lower()
    if any(w in original for w in ["오늘", "금일", "today"]):
        today = datetime.now(KST).date()
        today_only = [a for a in results if a["published_at"].astimezone(KST).date() == today]
        if today_only:
            results = today_only

    return results[:limit]
