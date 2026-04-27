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
한화생명 대체투자 포트폴리오 Bot
(Dataset 기준: 26.2월 / LookThrough 기준: 25.12월)

📊 [포트폴리오]
/조회 — 조건에 맞는 펀드 검색 (BS00000XXX 단독 입력도 가능)
/분석 — 비중·평균·그룹별 집계
/상세조회 BS00000XXX — 단일 펀드 모든 데이터 + 룩쓰루 요약 (펀드명 키워드도 OK)
/룩쓰루 BS00000XXX — 단일 펀드 하위자산 드릴다운 (펀드명 키워드도 OK)
/익스포저 [발행인|종목] X — 특정 발행인/종목 보유 펀드 역조회

📰 [뉴스]
/검색 키워드 — 키워드 뉴스 요약
/운용사뉴스 — 보유 운용사 기반 뉴스 리포트

📋 [업무 지시]
/등록 이름 — 팀원 등록
/지시 이름 | 업무내용 [| project=BS00001505]
/이력 — 최근 5건 / /이력 TASK-XXX 상세
/cancel — 업무 세션 종료

⚙️ [기타]
/help — 도움말
/refresh — DB 다시 불러오기

💡 조회/분석 직후 5분 이내엔 자연어 후속 가능: "1번 룩쓰루", "OpenAI 어디 있어?", "IRR 높은 순 3개"

[조회 예시]
/조회 미국 부동산 IRR 5% 이상 상위 10개
/조회 USD 약정 PD 수익률 높은 순
/조회 DPI 1배 이상 PE 회수율 순
/조회 미인출 100억 이상 큰 순
/조회 BTL 인프라
/조회 룩쓰루 가능한 PE 펀드
/조회 BS00000726, BS00001234

[분석 예시]
/분석 자산군별 평균 IRR
/분석 미국 부동산 전략별 평균 IRR
/분석 전체 포트폴리오에서 미국 비중

[필터 옵션]
자산군 / 지역 / 통화(KRW/USD/EUR/GBP) / 운용사 / 전략 / 섹터 /
투자유형(BTO/BTL/Buyout 등) / 세부유형 / 자본구조 / 빈티지 /
만기년도 / IRR / 약정 / 콜 / 상환 / 잔액 / NAV / DPI / TVPI /
인출률 / 미인출 / 룩쓰루 가능여부 / 트렌치 수
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
        handle_detail(db, chat_id, raw, ctx)
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
