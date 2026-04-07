import logging
import xml.etree.ElementTree as ET
import urllib.parse

from datetime import datetime
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo

import requests
from google import genai

import telegram_service
import config


KST = ZoneInfo("Asia/Seoul")


def build_effective_query(query: str) -> str:
    q = query.strip()

    # 사용자의 자연어를 시간조건으로 보정
    if any(word in q for word in ["오늘", "금일", "today"]):
        q = q.replace("오늘의", "").replace("오늘", "").replace("금일", "").strip()
        q = f"{q} when:1d"
    elif any(word in q.lower() for word in ["latest", "최근", "최신"]):
        q = f"{q} when:7d"

    return q.strip()


def parse_pub_date(pub_date: str):
    try:
        dt = parsedate_to_datetime(pub_date)
        if dt is None:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=KST)
        return dt
    except Exception:
        logging.exception("pubDate parse failed: %s", pub_date)
        return None


def normalize_title(title: str) -> str:
    return " ".join(title.lower().split())


def search_google_news_rss(query: str, limit: int = 20) -> list[dict]:
    effective_query = build_effective_query(query)
    encoded_query = urllib.parse.quote(effective_query)

    urls = [
        f"https://news.google.com/rss/search?q={encoded_query}&hl=ko&gl=KR&ceid=KR:ko",
        f"https://news.google.com/rss/search?q={encoded_query}&hl=en-US&gl=US&ceid=US:en",
    ]

    results = []
    seen_keys = set()

    for url in urls:
        try:
            resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()

            root = ET.fromstring(resp.text)
            items = root.findall(".//item")

            for item in items:
                title = (item.findtext("title") or "").strip()
                link = (item.findtext("link") or "").strip()
                pub_date = (item.findtext("pubDate") or "").strip()

                source_elem = item.find("source")
                source = source_elem.text.strip() if source_elem is not None and source_elem.text else ""

                published_at = parse_pub_date(pub_date)
                if not title or not published_at:
                    continue

                # 제목 + 언론사 기준으로 중복 완화
                dedup_key = (normalize_title(title), source.lower().strip())
                if dedup_key in seen_keys:
                    continue
                seen_keys.add(dedup_key)

                results.append({
                    "title": title,
                    "link": link,
                    "pub_date": pub_date,
                    "published_at": published_at,
                    "source": source,
                })

        except Exception:
            logging.exception("google news rss fetch failed: %s", url)

    # 최신순 정렬
    results.sort(key=lambda x: x["published_at"], reverse=True)

    # "오늘" 검색이면 한국시간 오늘 기사만 남김
    original_q = query.strip().lower()
    if any(word in original_q for word in ["오늘", "금일", "today"]):
        today_kr = datetime.now(KST).date()
        today_results = [
            a for a in results
            if a["published_at"].astimezone(KST).date() == today_kr
        ]
        if today_results:
            results = today_results

    return results[:limit]


def summarize_news_with_gemini(query: str, articles: list[dict]) -> str:
    if not config.GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY가 설정되지 않았습니다.")

    client = genai.Client(api_key=config.GEMINI_API_KEY)

    article_lines = []
    for idx, article in enumerate(articles, start=1):
        article_lines.append(
            f"{idx}. 제목: {article['title']}\n"
            f"   언론사: {article['source']}\n"
            f"   날짜: {article['published_at'].astimezone(KST).strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
            f"   링크: {article['link']}"
        )

    articles_text = "\n\n".join(article_lines)

    prompt = f"""
사용자 검색어: {query}

아래는 뉴스 기사 제목/언론사/발행시각 목록이다.
기사 본문은 제공되지 않았으므로, 제목 수준에서 확인 가능한 정보만 바탕으로 한국어로 정리하라.
확인되지 않은 세부 내용은 추정하지 말고, 불확실하면 보수적으로 표현하라.

출력 형식은 반드시 아래와 같이 맞춰라.

[3줄 요약]
1. ...
2. ...
3. ...

[핵심 포인트 5개]
- ...
- ...
- ...
- ...
- ...

작성 원칙:
- 최신 기사 흐름을 우선 반영
- 중복 기사 내용은 통합
- 과장 없이 팩트 중심
- 시장/산업 시사점은 짧게 반영
- 텔레그램에 바로 보내기 좋게 간결하게 작성
- 너무 길게 쓰지 말 것

뉴스 목록:
{articles_text}
"""

    resp = client.models.generate_content(
        model=config.GEMINI_MODEL,
        contents=prompt,
    )

    text = getattr(resp, "text", "") or ""
    return text.strip()


def build_news_summary(query: str, limit: int = 10) -> str:
    articles = search_google_news_rss(query, limit=limit)

    if not articles:
        return f"검색 결과가 없습니다.\n검색어: {query}"

    summary = summarize_news_with_gemini(query, articles)
    if not summary:
        return f"요약 생성에 실패했습니다.\n검색어: {query}"

    return summary


def handle_news_search_command(chat_id, text: str) -> None:
    query = text.replace("/검색", "", 1).strip()

    if not query:
        telegram_service.send_message(chat_id, "검색어를 같이 입력해주세요.\n예: /검색 오늘의 국내 주식시장")
        return

    try:
        result_text = build_news_summary(query, limit=10)
        telegram_service.send_long_message(chat_id, result_text)

    except Exception as e:
        logging.exception("news search failed")
        telegram_service.send_message(chat_id, f"뉴스 검색 중 오류가 발생했습니다.\n{e}")



# =========================================================
# 자동 뉴스 보고용 함수
# =========================================================
import os
import json
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import config
import telegram_service

KST = ZoneInfo("Asia/Seoul")


# ---------------------------
# 1. 상태 로드
# ---------------------------
def load_news_report_state():
    if not os.path.exists(config.NEWS_STATE_FILE):
        return {"sent_slots": []}
    try:
        with open(config.NEWS_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"sent_slots": []}


# ---------------------------
# 2. 상태 저장
# ---------------------------
def save_news_report_state(state):
    os.makedirs(os.path.dirname(config.NEWS_STATE_FILE), exist_ok=True)
    with open(config.NEWS_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ---------------------------
# 3. 현재 실행 슬롯 판단
# ---------------------------
def get_current_news_slot():
    now = datetime.now(KST)
    today = now.strftime("%Y-%m-%d")

    for t in config.NEWS_REPORT_TIMES:
        base = datetime.strptime(f"{today} {t}", "%Y-%m-%d %H:%M").replace(tzinfo=KST)
        if abs((now - base).total_seconds()) <= 600:  # ±10분
            return f"{today} {t}"

    return None


# ---------------------------
# 4. 키워드 전체 뉴스 수집
# ---------------------------
def collect_news_for_keywords():
    all_items = []
    seen = set()

    for kw in config.NEWS_KEYWORDS[:10]:
        try:
            items = search_google_news_rss(kw, limit=config.NEWS_PER_KEYWORD_LIMIT)

            for item in items:
                key = (item["title"].lower(), item.get("source", ""))
                if key in seen:
                    continue

                seen.add(key)
                item["keyword"] = kw
                all_items.append(item)

        except Exception:
            logging.exception(f"뉴스 수집 실패 | keyword={kw}")

    all_items.sort(key=lambda x: x["published_at"], reverse=True)

    return all_items[:config.NEWS_REPORT_MAX_ARTICLES]


# ---------------------------
# 5. Gemini 통합 요약
# ---------------------------
def summarize_news_batch_with_gemini(news_items):
    if not config.GEMINI_API_KEY:
        return "Gemini API 키 없음"

    from google import genai

    client = genai.Client(api_key=config.GEMINI_API_KEY)

    article_lines = []
    for i, item in enumerate(news_items, 1):
        article_lines.append(
            f"{i}. [{item['keyword']}] {item['title']} ({item.get('source','')})"
        )

   
    prompt = f"""
당신은 기관투자자에게 전달할 뉴스 요약 비서를 맡고 있다.

목표:
여러 키워드로 수집된 뉴스들을 단순 나열하지 말고,
중복 기사와 유사 이슈를 통합하여
"짧지만 똑똑한 요약" 형태로 정리하라.

중요:
과도한 해석이나 투자 판단은 하지 말고,
사실관계와 시장 흐름 중심으로만 정리하라.
다만, 단순 기사 제목 복붙 수준이 아니라
핵심 변화가 무엇인지 드러나게 요약하라.

[입력 뉴스 목록]
{chr(10).join(article_lines)}

[출력 형식]

[3줄 요약]
1. ...
2. ...
3. ...

[주요 이슈]
- ...
- ...
- ...

[세부 기사 요약]
1. [키워드] 기사 제목
   - 핵심 내용: ...
   - 의미: ...
2. [키워드] 기사 제목
   - 핵심 내용: ...
   - 의미: ...

[작성 원칙]
- 같은 내용의 중복 기사는 하나로 묶을 것
- 가장 최근 기사 흐름을 우선 반영할 것
- 기사 제목만 반복하지 말고, 공통된 흐름을 짧게 정리할 것
- 의미는 "시장 흐름상 어떤 성격의 뉴스인지" 정도만 짧게 쓸 것
- 과도한 전망, 투자 권고, 과장된 표현 금지
- 한국어로 간결하고 읽기 쉽게 작성
- 텔레그램 보고용이므로 너무 길지 않게 작성
"""


    resp = client.models.generate_content(
        model=config.GEMINI_MODEL,
        contents=prompt,
    )

    return (resp.text or "").strip()


# ---------------------------
# 6. 실행 함수 (메인에서 호출)
# ---------------------------
def maybe_run_scheduled_news(chat_id):
    if not config.NEWS_AUTO_REPORT_ENABLED:
        return

    slot = get_current_news_slot()
    if not slot:
        return

    state = load_news_report_state()
    sent = set(state.get("sent_slots", []))

    if slot in sent:
        return

    try:
        telegram_service.send_message(chat_id, f"🛰 뉴스 자동 보고 시작\n{slot}")

        news_items = collect_news_for_keywords()

        if not news_items:
            telegram_service.send_message(chat_id, "신규 뉴스 없음")
            return

        summary = summarize_news_batch_with_gemini(news_items)

        report = f"""📰 뉴스 자동 보고

[요약]
{summary}

[수집 기사 {len(news_items)}건]
"""

        for i, item in enumerate(news_items[:10], 1):
            report += f"\n{i}. {item['title']}\n   - {item['link']}"

        telegram_service.send_long_message(chat_id, report)

        state["sent_slots"].append(slot)
        state["sent_slots"] = state["sent_slots"][-20:]
        save_news_report_state(state)

    except Exception as e:
        logging.exception("뉴스 자동 보고 실패")
        telegram_service.send_message(chat_id, f"뉴스 오류: {e}")