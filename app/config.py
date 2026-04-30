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
GEMINI_MODEL = _env("GEMINI_MODEL", "gemini-3.1-flash-lite-preview")
GEMINI_FALLBACK_MODEL = _env("GEMINI_FALLBACK_MODEL", "gemini-2.5-flash-lite")
GEMINI_TIMEOUT_SECONDS = _env_int("GEMINI_TIMEOUT_SECONDS", 20)

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
NEWS_REPORT_TIMES = [t.strip() for t in _env("NEWS_REPORT_TIMES", "08:30,16:00").split(",") if t.strip()]
NEWS_PORTFOLIO_REPORT_TIMES = [t.strip() for t in _env("NEWS_PORTFOLIO_REPORT_TIMES", "09:00").split(",") if t.strip()]

NEWS_KEYWORDS = [
    "US interest rate Fed",
    "Korea interest rate BOK",
    "Credit Spread",
    "S&P 500",
    "KOSPI",
]

NEWS_PER_KEYWORD_LIMIT = _env_int("NEWS_PER_KEYWORD_LIMIT", 10)
NEWS_REPORT_MAX_ARTICLES = _env_int("NEWS_REPORT_MAX_ARTICLES", 30)
# 포트폴리오 뉴스 키워드 — GP 사용자 지정 + LookThrough 발행인 (PE/VC 한정).
# NEWS_GP_KEYWORDS 가 비어있으면 잔액 상위 자동 픽업으로 폴백.
# 예: NEWS_GP_KEYWORDS="Blackstone,KKR,Carlyle,Apollo,Brookfield,Ares,TPG"
NEWS_GP_KEYWORDS = [t.strip() for t in _env("NEWS_GP_KEYWORDS", "").split(",") if t.strip()]
NEWS_GP_OVERSEAS_LIMIT = _env_int("NEWS_GP_OVERSEAS_LIMIT", 6)
NEWS_GP_DOMESTIC_LIMIT = _env_int("NEWS_GP_DOMESTIC_LIMIT", 2)
NEWS_LOOKTHROUGH_LIMIT = _env_int("NEWS_LOOKTHROUGH_LIMIT", 8)
# LookThrough 발행인 추출 시 부모 펀드 자산군 화이트리스트.
# 기본 PE/VC — 부동산/인프라/사모대출 펀드의 발행인은 뉴스 신호가 약해 제외.
NEWS_LOOKTHROUGH_ASSET_CLASSES = [
    t.strip() for t in _env("NEWS_LOOKTHROUGH_ASSET_CLASSES", "PE,VC").split(",") if t.strip()
]
# 보고서에 키워드별 최소 보장 건수 — 해외/국내 운용사 비율 불균형으로 한쪽이 잘리는 걸 방지.
# round-robin 방식으로 각 키워드의 최신 N건을 우선 채운 뒤 NEWS_REPORT_MAX_ARTICLES 한도까지.
NEWS_PER_KEYWORD_REPORT_QUOTA = _env_int("NEWS_PER_KEYWORD_REPORT_QUOTA", 3)

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
