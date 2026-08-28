# 대체투자 포트폴리오 Telegram Bot

한화생명 대체투자 포트폴리오 조회/분석 + 뉴스 자동 보고 + 팀 업무 지시를 담당하는 Telegram 봇.
Render 무료 티어에서 FastAPI 웹훅 방식으로 상시 가동됩니다.

## 구조

```
app/
├── main.py              FastAPI 엔트리 (/webhook, /health, /cron/*)
├── config.py            환경변수
├── constants.py         자산군/지역/운용사 그룹 매핑
├── util.py              정규화·포맷 유틸
├── logger.py
├── db_engine.py         InvestmentDB (엑셀 로드·필터·분석)
├── services/
│   ├── gemini.py        싱글톤 Gemini 클라이언트
│   ├── telegram.py      Telegram API (Retry-After 대응)
│   ├── sheets.py        Google Sheets (Tasks, TaskHistory, Members)
│   ├── news_rss.py      Google News RSS 수집
│   └── file_extract.py  PDF/DOCX/TXT 텍스트 추출
├── parsers/
│   ├── query.py         /조회 자연어 → query_json
│   ├── analysis.py      /분석 자연어 → analysis_json
│   ├── task_eval.py     팀원 답변 평가
│   ├── news_summary.py  뉴스 요약 + 포트폴리오 임팩트
│   └── followup.py      멀티턴 후속 질문
├── handlers/
│   ├── router.py        명령어 분기
│   ├── query.py
│   ├── analysis.py
│   ├── news.py
│   ├── task.py          /지시 + /cancel + 답변 처리 + overdue/due
│   ├── team.py          /등록
│   └── detail.py        /상세조회 (단일 펀드 전 컬럼 + LT 요약)
├── formatters/          응답 텍스트 생성
├── state/               인메모리 저장소 (일일 한도, 대화 세션)
└── prompts/             외부화된 Gemini 프롬프트
```

## 환경변수

| 키 | 설명 |
|---|---|
| `TELEGRAM_TOKEN` | Telegram Bot Token |
| `TELEGRAM_WEBHOOK_SECRET` | 웹훅 URL 경로에 포함되는 시크릿 |
| `GEMINI_API_KEY` | Google Gemini API Key |
| `GEMINI_MODEL` | 기본 `gemini-3.1-flash-lite-preview` |
| `OWNER_CHAT_ID` | 사업부장 chat_id (권한 검사 + 뉴스 자동 보고 대상) |
| `GOOGLE_SA_JSON` | Google Service Account JSON (raw or base64) |
| `GOOGLE_SHEET_ID` | 업무 관리용 Google Sheet ID |
| `CRON_SECRET` | `/cron/*` 엔드포인트 Bearer 토큰 |
| `MAIN_DB_XLSX` | 메인 포트폴리오 Excel 경로 |
| `DAILY_QUESTION_LIMIT` | 사용자별 일일 조회/분석 한도 (기본 50) |
| `NEWS_REPORT_TIMES` | `09:10,15:30` 형태 |

## 배포 순서 (Render 무료)

1. **Google Cloud**
   - Service Account 생성 → JSON key 다운로드
   - 대상 Google Sheet에 해당 service account 이메일 "편집자"로 공유
   - `GOOGLE_SA_JSON` 환경변수에 JSON 전체를 붙여넣기 (또는 base64 인코딩)
   - `GOOGLE_SHEET_ID` 에 시트 ID (URL의 `/d/` 다음 부분)

2. **Render 웹 서비스**
   - GitHub repo 연결 → `render.yaml` 자동 인식
   - 환경변수 입력 (위 표 참고)
   - 빌드/배포 완료 후 발급된 도메인 확인

3. **Telegram 웹훅 등록**
   ```bash
   curl -X POST "https://<RENDER_URL>/admin/set-webhook?url=https://<RENDER_URL>/webhook/<SECRET>" \
     -H "Authorization: Bearer <CRON_SECRET>"
   ```
   또는 Telegram API를 직접 호출:
   ```bash
   curl "https://api.telegram.org/bot<TOKEN>/setWebhook?url=https://<RENDER_URL>/webhook/<SECRET>"
   ```

4. **GitHub Actions Secrets**
   - `RENDER_URL`: 배포된 Render URL
   - `CRON_SECRET`: Render 환경변수의 `CRON_SECRET`과 동일값
   - `.github/workflows/cron.yml` 이 자동으로 동작

5. **첫 /등록**
   - 팀원들이 각자 봇에 `/등록 이름` 전송 → Google Sheets Members 탭에 자동 등록

## 로컬 개발

```bash
pip install -r requirements.txt
export TELEGRAM_TOKEN=...
export GEMINI_API_KEY=...
export TELEGRAM_WEBHOOK_SECRET=dev-secret
export CRON_SECRET=dev-cron
export GOOGLE_SA_JSON="$(cat sa.json)"
export GOOGLE_SHEET_ID=...
uvicorn app.main:app --reload --port 8000
```

## 명령어

| 명령어 | 설명 |
|---|---|
| `/help` | 도움말 |
| `/조회 <자연어>` | 포트폴리오 조회 |
| `/분석 <자연어>` | 비중 및 자산군·지역·전략·빈티지·통화·투자유형별 분석 (한 번에 그룹 기준·지표 각각 최대 5개) |
| `/검색 <키워드>` | Google News 뉴스 요약 |
| `/등록 <이름>` | 팀원 등록 |
| `/지시 이름 \| 업무 [\| priority=high] [\| due=2026-04-20 10:00] [\| project=BS00001505]` | owner 전용 업무 지시 |
| `/cancel` | 진행 중 업무 세션 종료 |
| `/refresh` | Excel DB 재로드 |
| `/상세조회 <Project_ID 또는 펀드명>` | 단일 펀드 모든 데이터 + 룩쓰루 요약 |
| `/룩쓰루 <Project_ID 또는 펀드명>` | 단일 펀드 하위자산 드릴다운 |
| `/룩분석 [상위 개수]` | 조직 전체 룩쓰루 커버리지 및 통화·자산유형·발행인 집중도 분석 |
| `/익스포저 [발행인\|종목] <키워드>` | 특정 발행인/종목 보유 펀드 역조회 |

분석 한도 확대는 기존 `master_portfolio.xlsx`의 `Dataset` 컬럼을 조합해 집계하는 기능이므로,
지원 중인 그룹 기준과 지표를 사용하는 경우 **엑셀 파일의 컬럼이나 형식을 변경할 필요가 없습니다.**

### 룩쓰루 활용 예시

룩쓰루는 펀드 단위 분석에서 보이지 않는 **실제 하위자산 집중도와 중복 노출**을 확인할 때 유용합니다.

| 활용 목적 | 확인 내용 | 명령 예시 |
|---|---|---|
| 단일 펀드 진단 | 자산유형·통화 비중, 가중평균 보유금리, 만기구조, 상위 보유자산 | `/룩쓰루 BS00000726` |
| 전체 집중도 진단 | LT 커버리지, 통화·자산유형 구성, 상위 발행인 집중도 | `/룩분석 10` |
| 중복 익스포저 점검 | 같은 발행인이나 종목을 보유한 펀드와 조직 전체 LT 대비 비중 | `/익스포저 발행인 평택동부도로` |
| 종목명 포괄 검색 | 발행인 또는 보유종목명에 키워드가 포함된 모든 노출 | `/익스포저 종목 PGIM` |
| 뉴스 모니터링 | 주요 GP와 PE·VC 하위 발행인 관련 뉴스를 함께 점검 | `/포트폴리오뉴스` |
| 분석 대상 선별 | 룩쓰루 데이터가 있는 펀드만 먼저 조회한 뒤 개별 드릴다운 | `/조회 룩쓰루 가능한 PE 펀드` |

실무적으로는 **① 룩쓰루 가능 펀드 선별 → ② 개별 펀드의 상위 보유자산·만기 확인 →
③ 주요 발행인 역조회 → ④ 관련 뉴스 모니터링** 순서로 사용하면 집중위험 점검에 효과적입니다.
장부가 기준 집계이므로 시가·환율 환산 기준이 필요한 보고에는 원천 LookThrough 데이터에 해당 값과
기준일을 먼저 갖춰야 합니다.

조회/분석 직후 5분 이내에는 명령어 없이 자연어로 **후속 질문**이 가능합니다.
