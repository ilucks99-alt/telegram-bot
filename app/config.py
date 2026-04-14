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

# =========================================================
# Paths
# =========================================================
MAIN_DB_XLSX = _env("MAIN_DB_XLSX", "./Database/Data_Raw_2602.xlsx")
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
NOTIFY_OWNER_STATUS_UPDATES = _env_bool("NOTIFY_OWNER_STATUS_UPDATES", True)

# =========================================================
# Task workflow
# =========================================================
MAX_TASK_FEEDBACK_ROUND = _env_int("MAX_TASK_FEEDBACK_ROUND", 3)
TASK_NO_REPLY_MINUTES = _env_int("TASK_NO_REPLY_MINUTES", 30)
TASK_REPORT_COOLDOWN_MINUTES = _env_int("TASK_REPORT_COOLDOWN_MINUTES", 60)
TASK_DUE_REMINDER_MINUTES = _env_int("TASK_DUE_REMINDER_MINUTES", 30)

# =========================================================
# News auto-report
# =========================================================
NEWS_AUTO_REPORT_ENABLED = _env_bool("NEWS_AUTO_REPORT_ENABLED", True)
NEWS_REPORT_TIMES = [t.strip() for t in _env("NEWS_REPORT_TIMES", "09:10,15:30").split(",") if t.strip()]

NEWS_KEYWORDS = [
    "US interest rate Fed",
    "Korea interest rate BOK",
    "Credit Spread",
    "S&P 500",
    "KOSPI",
]

NEWS_PER_KEYWORD_LIMIT = _env_int("NEWS_PER_KEYWORD_LIMIT", 10)
NEWS_REPORT_MAX_ARTICLES = _env_int("NEWS_REPORT_MAX_ARTICLES", 20)

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
