import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import List, Optional

import requests

from app.logger import get_logger
from app.util import KST

logger = get_logger(__name__)


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


def search_google_news_rss(query: str, limit: int = 20) -> List[dict]:
    effective_query = build_effective_query(query)
    encoded = urllib.parse.quote(effective_query)

    urls = [
        f"https://news.google.com/rss/search?q={encoded}&hl=ko&gl=KR&ceid=KR:ko",
        f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en",
    ]

    results: List[dict] = []
    seen = set()

    for url in urls:
        try:
            resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            root = ET.fromstring(resp.text)

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

                results.append({
                    "title": title,
                    "link": link,
                    "pub_date": pub_date,
                    "published_at": published_at,
                    "source": source,
                })
        except Exception:
            logger.exception("google news rss fetch failed: %s", url)

    results.sort(key=lambda x: x["published_at"], reverse=True)

    original = query.strip().lower()
    if any(w in original for w in ["오늘", "금일", "today"]):
        today = datetime.now(KST).date()
        today_only = [a for a in results if a["published_at"].astimezone(KST).date() == today]
        if today_only:
            results = today_only

    return results[:limit]
