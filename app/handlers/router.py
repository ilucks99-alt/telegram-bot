from typing import Any, Dict

from app import config
from app.db_engine import InvestmentDB
from app.handlers.analysis import handle_analysis_command, handle_analysis_followup
from app.handlers.detail import handle_detail
from app.handlers.lookthrough import (
    handle_exposure_command,
    handle_exposure_followup,
    handle_lookthrough_command,
    handle_lookthrough_followup,
)
from app.handlers.news import handle_manager_news_command, handle_news_search_command
from app.handlers.query import handle_query_command, handle_search_followup
from app.handlers.task import (
    handle_cancel_command,
    handle_task_command,
    handle_task_document_reply,
    handle_task_history_command,
    handle_task_text_reply,
)
from app.handlers.team import handle_register_command
from app.logger import get_logger
from app.parsers.followup import parse_followup
from app.services import sheets
from app.services.telegram import send_message
from app.state import dialog_memory

logger = get_logger(__name__)


HELP_TEXT = """
한화생명 대체투자 포트폴리오 분석 및 조회 Bot 입니다.
(데이터 기준: 26.2월 / 펀드상세 PDF: 25.9월)

[안내]
- /분석: 비중/평균/그룹별 집계 등 포트폴리오 분석용입니다.
- /조회: 조건에 맞는 건을 조회합니다.
- /룩쓰루: 단일 펀드의 하위자산(자산유형/통화/금리/만기/TOP holdings) 드릴다운.
- /익스포저: 특정 발행인/종목이 포트폴리오 어디에 들어있는지 역방향 조회.
- /검색: 입력한 키워드 관련 뉴스를 요약, 정리하여 볼 수 있습니다.
- /운용사뉴스: 포트폴리오 상위 운용사 기반 뉴스 리포트를 즉시 생성합니다.
- 조회/분석 직후 5분 이내에는 자연어로 후속 질문이 가능합니다. (예: "그 중에 IRR 높은 순 3개")
- AI API 정책 상 조회/분석 수가 제한될 수 있습니다.

[사용 법]
/분석 포트폴리오 분석하고 싶은 내용
/조회 조회하고 싶은 내용
/룩쓰루 BS00000726
/익스포저 발행인 평택동부도로
/익스포저 종목 PGIM
/검색 검색 키워드
/운용사뉴스
/help 도움말
/refresh DB Refresh
/상세조회 BS00001505 (점검 중)

[업무 지시]
/등록 이름
/지시 이름 | 업무내용 [| project=BS00001505]
/이력 TASK-20260415-103000 (특정 업무 히스토리 조회)
/이력 (최근 완료 업무 5건)
/cancel (업무 세션 중 담당자가 종료)

[분석 예시]
/분석 전체 포트폴리오에서 미국 비중
/분석 자산군별 평균 IRR
/분석 미국 부동산 전략별 평균 IRR

[조회 예시]
/조회 KKR에 투자한 PE 펀드 중 2022년 Vintage
/조회 해외 부동산 core senior
/조회 블랙스톤 부동산 펀드
""".strip()


def _try_followup(db: InvestmentDB, chat_id: int, text: str) -> bool:
    ctx = dialog_memory.get_context(chat_id)
    if not ctx:
        return False

    parsed = parse_followup(
        kind=ctx["kind"],
        previous_payload=ctx["payload"],
        previous_summary=ctx.get("summary", ""),
        user_text=text,
        extras=ctx.get("extras") or {},
    )
    if not parsed:
        return False

    kind = parsed["kind"]
    payload = parsed["payload"]

    if kind == "query":
        handle_search_followup(db, chat_id, payload)
        return True
    if kind == "analysis":
        handle_analysis_followup(db, chat_id, payload)
        return True
    if kind == "lookthrough":
        handle_lookthrough_followup(db, chat_id, payload)
        return True
    if kind == "exposure":
        handle_exposure_followup(db, chat_id, payload)
        return True
    return False


def process_user_message(db: InvestmentDB, chat_id: int, text: str, ctx: Dict[str, Any]) -> None:
    raw = (text or "").strip()
    document = ctx.get("document")

    # 1) /cancel (업무 세션 중이 아니어도 동작)
    if raw == "/cancel":
        handle_cancel_command(db, chat_id)
        return

    # 슬래시 명령어는 항상 업무 답변보다 우선. 업무 세션 중에도 /help /지시 /조회 등을
    # 사용해야 하므로, 슬래시로 시작하는 텍스트는 task reply 라우팅을 건너뛴다.
    is_command = raw.startswith("/")

    # 2) 업무 세션 중 파일 답변
    if document and sheets.is_task_active(chat_id):
        try:
            handle_task_document_reply(db, chat_id, document)
        except Exception:
            logger.exception("task document reply failed")
            send_message(chat_id, "파일 처리 중 오류가 발생했습니다.")
        return

    # 3) 업무 세션 중 텍스트 답변 (슬래시 명령어는 제외)
    if raw and not is_command and sheets.is_task_active(chat_id):
        try:
            handle_task_text_reply(db, chat_id, raw)
        except Exception:
            logger.exception("task text reply failed")
            send_message(chat_id, "업무 답변 처리 중 오류가 발생했습니다.")
        return

    # 4) 팀원 등록
    if raw.startswith("/등록"):
        handle_register_command(chat_id, raw)
        return

    # 5) 업무 지시
    if raw.startswith("/지시"):
        if str(chat_id) != str(config.OWNER_CHAT_ID):
            send_message(chat_id, "너가 뭔데 지시하고 지랄이냐")
            return
        handle_task_command(db, chat_id, raw)
        return

    # 5-1) 업무 이력 조회 (owner 전용)
    if raw.startswith("/이력"):
        if str(chat_id) != str(config.OWNER_CHAT_ID):
            send_message(chat_id, "이력 조회 권한이 없습니다.")
            return
        handle_task_history_command(chat_id, raw)
        return

    # 6) 기존 명령어
    if raw == "/help":
        send_message(chat_id, HELP_TEXT)
        return

    if raw == "/refresh":
        try:
            db.refresh()
            send_message(chat_id, "엑셀 DB를 다시 불러왔습니다.")
        except Exception:
            logger.exception("refresh failed")
            send_message(chat_id, "DB 새로고침 중 오류가 발생했습니다.")
        return

    if raw.startswith("/상세조회"):
        handle_detail(chat_id, raw)
        return

    if raw.startswith("/조회"):
        handle_query_command(db, chat_id, raw, ctx)
        return

    if raw.startswith("/분석"):
        handle_analysis_command(db, chat_id, raw, ctx)
        return

    if raw.startswith("/룩쓰루"):
        handle_lookthrough_command(db, chat_id, raw, ctx)
        return

    if raw.startswith("/익스포저"):
        handle_exposure_command(db, chat_id, raw, ctx)
        return

    if raw.startswith("/운용사뉴스"):
        if str(chat_id) != str(config.OWNER_CHAT_ID):
            send_message(chat_id, "운용사 뉴스 호출 권한이 없습니다.")
            return
        handle_manager_news_command(db, chat_id)
        return

    if raw.startswith("/검색"):
        handle_news_search_command(chat_id, raw)
        return

    # 7) 명령어 없는 자유 텍스트 → 멀티턴 후속 질문 시도
    if raw and not raw.startswith("/"):
        if _try_followup(db, chat_id, raw):
            return

    send_message(
        chat_id,
        "지원하지 않는 명령어입니다.\n"
        "/조회, /분석, /상세조회, /검색, /등록, /지시 형식으로 입력해 주세요."
    )
