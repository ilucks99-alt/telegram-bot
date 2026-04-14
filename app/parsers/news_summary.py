from typing import Any, Dict, List

from app.constants import ASSET_CLASS_ALLOWED, REGION_ALLOWED
from app.logger import get_logger
from app.parsers import render_prompt, safe_json_parse
from app.services import gemini
from app.util import KST, format_amount_uk, format_pct

logger = get_logger(__name__)


def _format_articles(articles: List[dict]) -> str:
    lines = []
    for i, a in enumerate(articles, 1):
        ts = a["published_at"].astimezone(KST).strftime("%m-%d %H:%M")
        kw = a.get("keyword", "")
        prefix = f"[{kw}] " if kw else ""
        lines.append(f"{i}. {prefix}{a['title']} ({a.get('source','')}, {ts})")
    return "\n".join(lines)


def extract_entities_from_news(articles: List[dict]) -> List[Dict[str, Any]]:
    if not gemini.is_available() or not articles:
        return []
    prompt = render_prompt("news_entity_extractor.txt", articles=_format_articles(articles))
    raw = gemini.generate_json(prompt, max_output_tokens=1500, temperature=0.1)
    if not raw:
        return []
    try:
        data = safe_json_parse(raw)
    except Exception:
        logger.exception("news entity parse failed")
        return []

    items = data.get("items") or []
    cleaned: List[Dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        managers = [str(x).strip() for x in (it.get("managers") or []) if str(x).strip()]
        asset_classes = [x for x in (it.get("asset_classes") or []) if x in ASSET_CLASS_ALLOWED]
        regions = [x for x in (it.get("regions") or []) if x in REGION_ALLOWED]
        keyword = str(it.get("keyword") or "").strip()
        if not (managers or asset_classes or regions):
            continue
        cleaned.append({
            "keyword": keyword,
            "managers": managers[:5],
            "asset_classes": asset_classes[:3],
            "regions": regions[:3],
        })
    return cleaned


def match_portfolio_impact(entities: List[Dict[str, Any]], db) -> List[Dict[str, Any]]:
    out = []
    for e in entities:
        filters: Dict[str, Any] = {}
        if e.get("managers"):
            filters["manager"] = e["managers"]
        if e.get("asset_classes"):
            filters["asset_class"] = e["asset_classes"]
        if e.get("regions"):
            filters["region"] = e["regions"]
        if not filters:
            continue
        impact = db.portfolio_impact_summary(filters)
        if impact.get("count", 0) == 0:
            continue
        out.append({
            "keyword": e["keyword"],
            "filters": filters,
            "count": impact["count"],
            "sum_commitment": format_amount_uk(impact["sum_commitment"]),
            "sum_outstanding": format_amount_uk(impact["sum_outstanding"]),
            "sum_nav": format_amount_uk(impact["sum_nav"]),
            "avg_irr": format_pct(impact["avg_irr"]),
        })
    return out


def summarize_news(query: str, articles: List[dict], portfolio_impact: List[Dict[str, Any]] = None) -> str:
    if not gemini.is_available():
        return "Gemini 미설정"
    if not articles:
        return f"검색 결과가 없습니다.\n검색어: {query}"

    prompt = render_prompt(
        "news_summarizer.txt",
        query=query,
        articles=_format_articles(articles),
        portfolio_impact=portfolio_impact or [],
    )
    text = gemini.generate_text(prompt, max_output_tokens=2048, temperature=0.3)
    return text or f"요약 생성에 실패했습니다.\n검색어: {query}"
