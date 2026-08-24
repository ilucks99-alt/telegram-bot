import os


def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


def _env_int(key: str, default: int) -> int:
    try:
        return int(_env(key, str(default)))
    except ValueError:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(_env(key, str(default)))
    except ValueError:
        return default


def _env_bool(key: str, default: bool) -> bool:
    v = _env(key, "1" if default else "0").lower()
    return v in ("1", "true", "yes", "on")


# =========================================================
# Telegram / Gemini
# =========================================================
TELEGRAM_TOKEN = _env("TELEGRAM_TOKEN")
TELEGRAM_WEBHOOK_SECRET = _env("TELEGRAM_WEBHOOK_SECRET", "change-me")
GEMINI_API_KEY = _env("GEMINI_API_KEY")
FRED_API_KEY = _env("FRED_API_KEY")  # 매크로 지표용 (https://fred.stlouisfed.org/docs/api/api_key.html)
GEMINI_MODEL = _env("GEMINI_MODEL", "gemini-2.5-flash")
GEMINI_FALLBACK_MODEL = _env("GEMINI_FALLBACK_MODEL", "gemini-2.5-flash-lite")
GEMINI_TIMEOUT_SECONDS = _env_int("GEMINI_TIMEOUT_SECONDS", 20)
# Vertex AI 모드 — Render egress IP 가 Gemini Developer API 에서 차단될 때 우회용.
# GOOGLE_SA_JSON 의 service account 자격증명을 그대로 재사용 (Vertex AI User 역할 필요).
# project_id 는 SA JSON 안에서 자동 추출.
USE_VERTEX_AI = _env_bool("USE_VERTEX_AI", False)
VERTEX_LOCATION = _env("VERTEX_LOCATION", "us-central1")

# =========================================================
# Paths
# =========================================================
MAIN_DB_XLSX = _env("MAIN_DB_XLSX", "./Database/master_portfolio.xlsx")
MAIN_DB_SHEET = _env("MAIN_DB_SHEET", "Dataset")
LT_SHEET = _env("LT_SHEET", "LookThrough")
DETAIL_XLSX = _env("DETAIL_XLSX", "./Database/Investment_Raw_2509.xlsx")
TELEGRAM_FILE_DIR = _env("TELEGRAM_FILE_DIR", "/tmp/telegram_files")

# =========================================================
# Query / Analysis
# =========================================================
DEFAULT_LIMIT = _env_int("DEFAULT_LIMIT", 9999)
MAX_LIMIT = _env_int("MAX_LIMIT", 9999)
SEARCH_CONFIRM_THRESHOLD = _env_int("SEARCH_CONFIRM_THRESHOLD", 30)
DAILY_QUESTION_LIMIT = _env_int("DAILY_QUESTION_LIMIT", 50)

# =========================================================
# Owner / Permissions
# =========================================================
OWNER_CHAT_ID = _env("OWNER_CHAT_ID", "315716158")

# =========================================================
# Task workflow
# =========================================================
MAX_TASK_FEEDBACK_ROUND = _env_int("MAX_TASK_FEEDBACK_ROUND", 3)
TASK_NO_REPLY_MINUTES = _env_int("TASK_NO_REPLY_MINUTES", 30)
TASK_REPORT_COOLDOWN_MINUTES = _env_int("TASK_REPORT_COOLDOWN_MINUTES", 60)
TASK_QUEUE_MAX = _env_int("TASK_QUEUE_MAX", 5)
# 담당자가 [확인했습니다] 버튼을 누르지 않은 채 N분이 지나면 owner 에게 알림.
# TASK_NO_REPLY_MINUTES 보다 작아야 의미가 있다(그 시점이 되면 overdue 가 가져감).
TASK_UNACK_ALERT_MINUTES = _env_int("TASK_UNACK_ALERT_MINUTES", 15)
# 마감(due_at) N분 전에 담당자/owner 에게 푸시.
TASK_DUE_REMINDER_MINUTES = _env_int("TASK_DUE_REMINDER_MINUTES", 30)

# =========================================================
# News auto-report
# =========================================================
NEWS_AUTO_REPORT_ENABLED = _env_bool("NEWS_AUTO_REPORT_ENABLED", True)
# 아침에는 시장+포트폴리오 통합 브리핑, 오후에는 시장 업데이트만 보낸다.
NEWS_MORNING_BRIEFING_TIMES = [
    t.strip() for t in _env("NEWS_MORNING_BRIEFING_TIMES", "09:00").split(",") if t.strip()
]
NEWS_REPORT_TIMES = [t.strip() for t in _env("NEWS_REPORT_TIMES", "16:00").split(",") if t.strip()]

# 거시 뉴스 키워드 — 대체투자 PM 관점 시그널 기준 기본 12개.
# 콤마(,) 구분 env `NEWS_KEYWORDS` 로 override 가능. 콤마 포함 키워드는 사용 불가.
NEWS_KEYWORDS = [t.strip() for t in _env(
    "NEWS_KEYWORDS",
    "Federal Reserve rate decision,"
    "Bank of Korea rate decision,"
    "US Treasury 10 year yield,"
    "US CPI inflation,"
    "credit spread high yield,"
    "S&P 500,"
    "KOSPI,"
    "원달러 환율,"
    "WTI crude oil,"
    "gold price,"
    "private equity,"
    "commercial real estate"
).split(",") if t.strip()]

NEWS_PER_KEYWORD_LIMIT = _env_int("NEWS_PER_KEYWORD_LIMIT", 10)
NEWS_REPORT_MAX_ARTICLES = _env_int("NEWS_REPORT_MAX_ARTICLES", 50)
# 포트폴리오 뉴스 키워드 — GP 사용자 지정 + LookThrough 발행인 (PE/VC 한정).
# NEWS_GP_KEYWORDS 가 비어있으면 잔액 상위 자동 픽업으로 폴백.
# 예: NEWS_GP_KEYWORDS="Blackstone,KKR,Carlyle,Apollo,Brookfield,Ares,TPG"
NEWS_GP_KEYWORDS = [t.strip() for t in _env("NEWS_GP_KEYWORDS", "").split(",") if t.strip()]
NEWS_GP_OVERSEAS_LIMIT = _env_int("NEWS_GP_OVERSEAS_LIMIT", 6)
NEWS_GP_DOMESTIC_LIMIT = _env_int("NEWS_GP_DOMESTIC_LIMIT", 2)
NEWS_LOOKTHROUGH_LIMIT = _env_int("NEWS_LOOKTHROUGH_LIMIT", 30)
# LookThrough 발행인 추출 시 부모 펀드 자산군 화이트리스트.
# 기본 PE/VC — 부동산/인프라/사모대출 펀드의 발행인은 뉴스 신호가 약해 제외.
NEWS_LOOKTHROUGH_ASSET_CLASSES = [
    t.strip() for t in _env("NEWS_LOOKTHROUGH_ASSET_CLASSES", "PE,VC").split(",") if t.strip()
]
# 보고서에 키워드별 최소 보장 건수 — 해외/국내 운용사 비율 불균형으로 한쪽이 잘리는 걸 방지.
# round-robin 방식으로 각 키워드의 최신 N건을 우선 채운 뒤 NEWS_REPORT_MAX_ARTICLES 한도까지.
NEWS_PER_KEYWORD_REPORT_QUOTA = _env_int("NEWS_PER_KEYWORD_REPORT_QUOTA", 3)
NEWS_MORNING_MARKET_ARTICLE_LIMIT = _env_int("NEWS_MORNING_MARKET_ARTICLE_LIMIT", 20)
NEWS_MORNING_PORTFOLIO_ARTICLE_LIMIT = _env_int("NEWS_MORNING_PORTFOLIO_ARTICLE_LIMIT", 30)

# 딜사이트·더벨 대체투자 전문 브리핑. Google News의 site: 검색을
# 이용해 제목/링크를 수집하며, 원문 유료 기사를 우회하지 않는다.
ALTERNATIVE_NEWS_REPORT_TIMES = [
    t.strip() for t in _env("ALTERNATIVE_NEWS_REPORT_TIMES", "08:30").split(",") if t.strip()
]
ALTERNATIVE_NEWS_LOOKBACK_HOURS = _env_int("ALTERNATIVE_NEWS_LOOKBACK_HOURS", 36)
ALTERNATIVE_NEWS_PER_QUERY_LIMIT = _env_int("ALTERNATIVE_NEWS_PER_QUERY_LIMIT", 10)
ALTERNATIVE_NEWS_MAX_ARTICLES = _env_int("ALTERNATIVE_NEWS_MAX_ARTICLES", 20)
# 아래 각 항목이 하나의 별도 RSS 검색어다. 콤마로 합쳐진 한 개의
# 긴 검색어가 아니다. 예: `site:thebell.co.kr (IPO OR 상장 OR 중복상장) when:2d`.
_ALTERNATIVE_NEWS_DEFAULT_TOPICS = (
    "대체투자",
    "사모펀드 OR PEF",
    "M&A OR 인수합병",
    "IPO OR 상장 OR 중복상장",
    "회수 OR 엑시트",
    "블라인드펀드 OR 펀드결성",
    "부동산 OR 인프라",
    "사모대출 OR 인수금융",
    "연기금 OR 공제회 OR LP",
    "금융위 OR 공정위 OR 자본시장 규제",
)
_alternative_news_topics_env = _env("ALTERNATIVE_NEWS_TOPICS")
ALTERNATIVE_NEWS_TOPICS = (
    [t.strip() for t in _alternative_news_topics_env.split(",") if t.strip()]
    if _alternative_news_topics_env
    else list(_ALTERNATIVE_NEWS_DEFAULT_TOPICS)
)

# =========================================================
# Google Sheets
# =========================================================
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON", "")
GOOGLE_SHEET_ID = _env("GOOGLE_SHEET_ID", "")

# =========================================================
# Cron
# =========================================================
CRON_SECRET = _env("CRON_SECRET", "change-me")

# =========================================================
# Dialog memory
# =========================================================
DIALOG_MEMORY_TTL_SECONDS = _env_int("DIALOG_MEMORY_TTL_SECONDS", 300)
