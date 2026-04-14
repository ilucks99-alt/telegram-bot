from app.services.telegram import send_message


def handle_detail(chat_id, raw: str) -> None:
    send_message(
        chat_id,
        "[안내] /상세조회 기능은 현재 점검 중입니다.\n"
        "Windows 전용 Excel 변환을 대체하는 크로스플랫폼 구현이 준비되는 대로 재개됩니다."
    )
