import os


# =========================================================
# 환경설정
# =========================================================

# 운영 시 환경변수 사용 권장
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
#TELEGRAM_TOKEN = "8626745530:AAErOzyIGiDj0DmQ0PjavJssfAXZbVPIemc"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()

OFFSET_FILE = os.getenv("OFFSET_FILE", "telegram_offset.txt").strip()
LOCK_FILE = os.getenv("LOCK_FILE", "telegram_bot.lock").strip()
LOG_FILE = os.getenv("LOG_FILE", "telegram_bot.log").strip()

MAIN_DB_XLSX = os.getenv(
    "MAIN_DB_XLSX",
    "./Database/Data_Raw_2602.xlsx"
).strip()

DETAIL_XLSX = os.getenv(
    "DETAIL_XLSX",
    "./Database/Investment_Raw_2509.xlsx"
).strip()

POLL_TIMEOUT = int(os.getenv("POLL_TIMEOUT", "50"))
POLL_SLEEP_ON_ERROR = float(os.getenv("POLL_SLEEP_ON_ERROR", "3"))

DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "9999"))
MAX_LIMIT = int(os.getenv("MAX_LIMIT", "9999"))

QUESTION_LIMIT_FILE = os.getenv("QUESTION_LIMIT_FILE", "daily_question_usage.json").strip()
DAILY_QUESTION_LIMIT = int(os.getenv("DAILY_QUESTION_LIMIT", "50"))

OWNER_CHAT_ID = os.getenv("OWNER_CHAT_ID", "315716158").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite-preview").strip()
#GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite").strip()

# 업무 지시용 
TEAM_MEMBER_FILE = os.getenv("TEAM_MEMBER_FILE", "team_members.json").strip()
TASK_SESSION_FILE = os.getenv("TASK_SESSION_FILE", "task_sessions.json").strip()
TELEGRAM_FILE_DIR = os.getenv("TELEGRAM_FILE_DIR", "telegram_files").strip()
MAX_TASK_FEEDBACK_ROUND = int(os.getenv("MAX_TASK_FEEDBACK_ROUND", "3"))

# News 업데이트
NEWS_AUTO_REPORT_ENABLED = True
NEWS_REPORT_TIMES = ["09:10", "15:30"]

NEWS_KEYWORDS = [
    "US interest rate Fed",
    "Korea interest rate BOK",
    "Credit Spread"
    "S&P 500",
    "KOSPI"
]

NEWS_PER_KEYWORD_LIMIT = 10
NEWS_REPORT_MAX_ARTICLES = 20
NEWS_STATE_FILE = "state/news_report_state.json"

TASK_NO_REPLY_MINUTES = 30
TASK_REPORT_COOLDOWN_MINUTES = 60
