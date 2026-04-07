import os
import json
import time
import atexit
import signal
import logging
import config
import requests
import pandas as pd
import util
import logging
from typing import Any, Dict, List, Optional
from telegram_service import get_updates, send_message, save_offset, load_offset
from news_service import handle_news_search_command
from team_service import register_team_member, find_team_member_chat_id
from task_service import (
    create_task_session,
    send_task_to_assignee,
    is_active_task_session,
    handle_task_text_reply,
    handle_task_document_reply,
)
from news_service import handle_news_search_command, maybe_run_scheduled_news
from task_service import check_and_report_overdue_tasks
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

try:
    from google import genai
    from google.genai import types
except Exception:
    genai = None
    types = None

# =========================================================
# 로깅
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(config.LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# =========================================================
# 기본 검증 / Gemini 설정
# =========================================================
if not config.TELEGRAM_TOKEN:
    raise ValueError("TELEGRAM_TOKEN 환경변수가 비어 있습니다.")

if not os.path.exists(config.MAIN_DB_XLSX):
    raise FileNotFoundError(f"메인 DB 파일이 없습니다: {config.MAIN_DB_XLSX}")

if not os.path.exists(config.DETAIL_XLSX):
    logging.warning("상세 엑셀 파일이 없습니다. /상세조회 기능은 실패할 수 있습니다: %s", config.DETAIL_XLSX)

USE_GEMINI = bool(config.GEMINI_API_KEY and genai is not None and types is not None)
client = genai.Client(api_key=config.GEMINI_API_KEY) if USE_GEMINI else None

if USE_GEMINI:
    logging.info("Gemini 사용 가능 | model=%s", config.GEMINI_MODEL)
else:
    logging.info("Gemini 미사용 모드")


# =========================================================
# 단일 실행 락
# =========================================================
def acquire_lock(lock_path: str) -> None:
    if os.path.exists(lock_path):
        try:
            with open(lock_path, "r", encoding="utf-8") as f:
                pid_str = f.read().strip()
            if pid_str.isdigit():
                old_pid = int(pid_str)
                try:
                    os.kill(old_pid, 0)
                    raise RuntimeError(
                        f"이미 실행 중인 프로세스가 있습니다. PID={old_pid}. "
                        "동일 봇 중복 실행 시 Telegram 409 오류가 발생할 수 있습니다."
                    )
                except OSError:
                    pass
        except Exception:
            pass

    with open(lock_path, "w", encoding="utf-8") as f:
        f.write(str(os.getpid()))

    def _cleanup():
        try:
            if os.path.exists(lock_path):
                os.remove(lock_path)
        except Exception:
            pass

    atexit.register(_cleanup)

    def _signal_handler(signum, frame):
        _cleanup()
        raise KeyboardInterrupt()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)


# =========================================================
# 기준 정의
# =========================================================
ASSET_CLASS_ALLOWED = {"Real_Estate", "PE", "VC", "PD", "Infrastructure"}
REGION_ALLOWED = {"US", "Europe", "Asia", "Global", "KOR", "MENA", "Canada"}
OVERSEAS_REGIONS = ["US", "Europe", "Asia", "Global", "MENA", "Canada"]

SORT_BY_ALLOWED = {"irr", "commitment", "called", "outstanding", "nav", "maturity_year"}
SORT_ORDER_ALLOWED = {"asc", "desc"}

ANALYSIS_TYPE_ALLOWED = {"share", "grouped_metric"}
GROUPBY_ALLOWED = {"asset_class", "region", "strategy", "manager", "sector", "vintage", "maturity_year"}
ANALYSIS_METRIC_ALLOWED = {
    "commitment",
    "called",
    "outstanding",
    "nav",
    "count",
    "irr_avg",
    "irr_weighted_commitment",
    "irr_weighted_called",
    "irr_weighted_outstanding",
    "irr_weighted_nav",
}

QUERY_SCHEMA_PROMPT = """
{
  "mode": "query" | "advice",
  "query_json": {
    "query_type": "summary_with_list",
    "filters": {
      "asset_class": [string],
      "manager": [string],
      "region": [string],
      "strategy": [string],
      "sector": [string],
      "project_id": [string],
      "fund_name_keywords": [string],
      "asset_name_keywords": [string],
      "vintage_from": integer,
      "vintage_to": integer,
      "maturity_year_from": integer,
      "maturity_year_to": integer,
      "irr_min": number,
      "irr_max": number,
      "commit_min": number,
      "commit_max": number,
      "called_min": number,
      "called_max": number,
      "outstanding_min": number,
      "outstanding_max": number,
      "nav_min": number,
      "nav_max": number
    },
    "sort": {
      "by": "irr" | "commitment" | "called" | "outstanding" | "nav" | "maturity_year",
      "order": "asc" | "desc"
    },
    "output": {
      "include_summary": true,
      "include_list": true,
      "limit": integer
    }
  },
  "advice_text": string | null
}
""".strip()

ANALYSIS_SCHEMA_PROMPT = """
{
  "mode": "analysis" | "advice",
  "analysis_json": {
    "analysis_type": "share" | "grouped_metric",

    "base_filters": {
      "asset_class": [string],
      "manager": [string],
      "region": [string],
      "strategy": [string],
      "sector": [string],
      "project_id": [string],
      "fund_name_keywords": [string],
      "asset_name_keywords": [string],
      "vintage_from": integer,
      "vintage_to": integer,
      "maturity_year_from": integer,
      "maturity_year_to": integer,
      "irr_min": number,
      "irr_max": number,
      "commit_min": number,
      "commit_max": number,
      "called_min": number,
      "called_max": number,
      "outstanding_min": number,
      "outstanding_max": number,
      "nav_min": number,
      "nav_max": number
    },

    "target_filters": {
      "asset_class": [string],
      "manager": [string],
      "region": [string],
      "strategy": [string],
      "sector": [string],
      "project_id": [string],
      "fund_name_keywords": [string],
      "asset_name_keywords": [string],
      "vintage_from": integer,
      "vintage_to": integer,
      "maturity_year_from": integer,
      "maturity_year_to": integer,
      "irr_min": number,
      "irr_max": number,
      "commit_min": number,
      "commit_max": number,
      "called_min": number,
      "called_max": number,
      "outstanding_min": number,
      "outstanding_max": number,
      "nav_min": number,
      "nav_max": number
    },

    "metric": "commitment" | "called" | "outstanding" | "nav" | "count" |
              "irr_avg" | "irr_weighted_commitment" | "irr_weighted_called" |
              "irr_weighted_outstanding" | "irr_weighted_nav",

    "groupby": [string],
    "metrics": [string],
    "sort_by": string,
    "top_n": integer,
    "sort_order": "asc" | "desc"
  },
  "advice_text": string | null
}
""".strip()

# =========================================================
# 운용사 그룹 / 별칭 매핑
# =========================================================
MANAGER_GROUP_MEMBERS = {
    "BlackRock": [
        "BlackRock",
        "HPS",
        "HPS Investment",
        "HPS Investment Partners",
        "GIP",
        "Global Infrastructure Partners",
    ],
    "Blackstone": [
        "Blackstone",
        "GSO",
    ],
    "TPG": [
        "TPG",
        "Angelo Gordon",
        "TPG Angelo Gordon",
    ],
    "Apollo": [
        "Apollo",
        "Bridge Investment Group",
    ],
    "Ares": [
        "Ares",
        "Ares Management",
        "Ares Capital",
    ],
    "New York Life": [
        "New York Life",
        "Apogem",
        "Apogem Capital",
        "Apogem Capital LLC",
        "MCF",
        "Goldpoint",
        "Madison Capital Funding",
    ],
    "Morgan Stanley": [
        "Morgan Stanley",
        "Mesa West",
        "Mesa West Capital",
    ],
    "Allianz": [
        "Allianz",
        "PIMCO",
    ],
    "Partners Group": [
        "Partners Group",
        "Partners Holding AG",
    ],
}

MANAGER_GROUP_ALIASES = {
    "BlackRock": [
        "블랙락",
        "blackrock",
        "black rock",
        "hps",
        "hps investment",
        "gip",
        "global infrastructure partners",
    ],
    "Blackstone": [
        "블랙스톤",
        "blackstone",
        "gso",
    ],
    "TPG": [
        "tpg",
        "angelo gordon"
        "tpg angelo gordon"
    ],
    "Apollo": [
        "apollo",
        "bridge investment group",
    ],
    "Ares": [
        "ares",
        "ares management",
        "ares capital",
    ],
    "New York Life": [
        "new york life",
        "nylim",
        "apogem",
        "mcf",
        "goldpoint",
        "madison capital funding",
    ],
    "Morgan Stanley": [
        "morgan stanley",
        "mesa west",
        "mesa west capital",
    ],
    "Allianz": [
        "allianz",
        "pimco",
    ],
    "Partners Group": [
        "partners group",
        "partners holding ag",
    ],
}

def _build_manager_group_maps() -> tuple[Dict[str, str], Dict[str, List[str]]]:
    alias_to_group: Dict[str, str] = {}
    group_to_keywords: Dict[str, List[str]] = {}

    for group_name, members in MANAGER_GROUP_MEMBERS.items():
        merged = []
        merged.extend([group_name])
        merged.extend(members)
        merged.extend(MANAGER_GROUP_ALIASES.get(group_name, []))

        dedup_norm = set()
        keywords: List[str] = []

        for item in merged:
            s = str(item).strip()
            if not s:
                continue

            ns = util.normalize_text(s)
            if not ns:
                continue

            if ns not in dedup_norm:
                dedup_norm.add(ns)
                keywords.append(s)

            alias_to_group[ns] = group_name

        group_to_keywords[group_name] = keywords

    return alias_to_group, group_to_keywords


MANAGER_ALIAS_TO_GROUP, MANAGER_GROUP_KEYWORDS = _build_manager_group_maps()

# =========================================================
# 업무 지시 관리용
# =========================================================


def handle_register_command(chat_id: int, raw: str) -> None:
    name = raw.replace("/등록", "", 1).strip()

    if not name:
        send_message(chat_id, "형식: /등록 홍길동")
        return

    register_team_member(chat_id, name)
    send_message(chat_id, f"{name} 등록이 완료되었습니다.")

def handle_task_command(owner_chat_id: int, raw: str) -> None:
    payload = raw.replace("/지시", "", 1).strip()


    if "|" not in payload:
        send_message(owner_chat_id, "형식: /지시 이름 | 업무내용")
        return

    assignee_name, instruction = [x.strip() for x in payload.split("|", 1)]

    assignee_chat_id = find_team_member_chat_id(assignee_name)
    if not assignee_chat_id:
        send_message(owner_chat_id, f"담당자 등록 정보를 찾지 못했습니다: {assignee_name}\n먼저 해당 팀원이 /등록 이름 으로 등록해야 합니다.")
        return

    session = create_task_session(
        owner_chat_id=owner_chat_id,
        assignee_chat_id=assignee_chat_id,
        assignee_name=assignee_name,
        instruction=instruction,
    )

    send_task_to_assignee(session)
    send_message(
        owner_chat_id,
        f"업무 지시를 전송했습니다.\n- 담당자: {assignee_name}\n- 업무번호: {session['task_id']}"
    )

# =========================================================
# DB 로더 / 검색기 / 분석기
# =========================================================
class InvestmentDB:
    def __init__(self, path: str):
        self.path = path
        self.df = self._load()

    def refresh(self) -> None:
        self.df = self._load()

    def _load(self) -> pd.DataFrame:
        df = pd.read_excel(self.path)

        expected_cols = [
            "Project_ID", "Asset_Class", "Asset_Name", "Manager", "Region",
            "Strategy", "Sector", "Initial_Date", "Vintage",
            "Investment_Period", "Maturity_Date", "Maturity_Year",
            "Commitment", "Called", "Outstanding", "NAV", "IRR"
        ]
        missing = [c for c in expected_cols if c not in df.columns]
        if missing:
            raise ValueError(f"필수 컬럼 누락: {missing}")

        text_cols = ["Project_ID", "Asset_Class", "Asset_Name", "Manager", "Region", "Strategy", "Sector"]
        for c in text_cols:
            df[c] = df[c].fillna("").astype(str).str.strip()

        date_cols = ["Initial_Date", "Investment_Period", "Maturity_Date"]
        for c in date_cols:
            df[c] = pd.to_datetime(df[c], errors="coerce")

        num_cols = ["Vintage", "Maturity_Year", "Commitment", "Called", "Outstanding", "NAV", "IRR"]
        for c in num_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df["Asset_Class_Std"] = df["Asset_Class"].apply(self._std_asset_class)
        df["Region_Std"] = df["Region"].apply(self._std_region)
        df["Manager_Norm"] = df["Manager"].apply(util.normalize_text)
        df["Asset_Name_Norm"] = df["Asset_Name"].apply(util.normalize_text)
        df["Strategy_Norm"] = df["Strategy"].apply(util.normalize_text)
        df["Sector_Norm"] = df["Sector"].apply(util.normalize_text)
        df["Project_ID_Norm"] = df["Project_ID"].apply(util.normalize_text)

        logging.info("DB loaded | rows=%s | file=%s", len(df), self.path)
        return df

    def _std_asset_class(self, x: str) -> str:
        nx = util.normalize_text(x)
        mapping = {
            "부동산": "Real_Estate",
            "해외부동산": "Real_Estate",
            "realestate": "Real_Estate",
            "real_estate": "Real_Estate",
            "re": "Real_Estate",
            "realasset": "Real_Estate",
            "realassets": "Real_Estate",

            "pe": "PE",
            "privateequity": "PE",
            "private_equity": "PE",
            "사모펀드": "PE",

            "vc": "VC",
            "venturecapital": "VC",
            "venture_capital": "VC",
            "벤처": "VC",

            "pd": "PD",
            "privatedebt": "PD",
            "private_debt": "PD",
            "privatecredit": "PD",
            "private_credit": "PD",
            "사모대출": "PD",

            "infra": "Infrastructure",
            "infrastructure": "Infrastructure",
            "인프라": "Infrastructure",
        }
        return mapping.get(nx, x)

    def _std_region(self, x: str) -> str:
        nx = util.normalize_text(x)
        mapping = {
            "미국": "US",
            "북미": "US",
            "us": "US",
            "usa": "US",
            "unitedstates": "US",

            "유럽": "Europe",
            "europe": "Europe",
            "eu": "Europe",

            "아시아": "Asia",
            "asia": "Asia",

            "글로벌": "Global",
            "global": "Global",
            "전세계": "Global",

            "국내": "KOR",
            "한국": "KOR",
            "kor": "KOR",
            "korea": "KOR",

            "중동": "MENA",
            "mena": "MENA",
            "middleeast": "MENA",

            "캐나다": "Canada",
            "canada": "Canada",
        }
        return mapping.get(nx, x)

    
    def _expand_manager_keywords(self, managers: List[str]) -> List[str]:
        expanded: List[str] = []

        for m in managers:
            raw = str(m).strip()
            if not raw:
                continue

            norm = util.normalize_text(raw)
            group_name = MANAGER_ALIAS_TO_GROUP.get(norm)

            # 1) 그룹으로 인식되면 그룹 전체 확장
            if group_name:
                expanded.extend(MANAGER_GROUP_KEYWORDS.get(group_name, []))
                continue

            # 2) 그룹으로 안 잡혀도 부분 일치로 한 번 더 시도
            matched_group = None
            for alias_norm, gname in MANAGER_ALIAS_TO_GROUP.items():
                if norm == alias_norm or norm in alias_norm or alias_norm in norm:
                    matched_group = gname
                    break

            if matched_group:
                expanded.extend(MANAGER_GROUP_KEYWORDS.get(matched_group, []))
            else:
                expanded.append(raw)

        # 정규화 기준 중복 제거
        seen = set()
        out = []
        for x in expanded:
            nx = util.normalize_text(x)
            if nx and nx not in seen:
                seen.add(nx)
                out.append(x)

        logging.info(
            "manager expansion | input=%s | expanded=%s",
            managers,
            out
        )
        return out

    
    
    def _exclude_matured(self, df: pd.DataFrame) -> pd.DataFrame:
        current_year = util.get_kst_today_year()
        return df[
            df["Maturity_Year"].isna() |
            (df["Maturity_Year"] >= current_year)
        ].copy()

    def _apply_filters(self, base_df: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
        df = base_df.copy()

        asset_classes = filters.get("asset_class") or []
        if asset_classes:
            df = df[df["Asset_Class_Std"].isin(asset_classes)]

        regions = filters.get("region") or []
        if regions:
            df = df[df["Region_Std"].isin(regions)]

        # 전략: AND 조건
        strategies = filters.get("strategy") or []
        if strategies:
            mask = pd.Series([True] * len(df), index=df.index)
            for s in strategies:
                mask &= util.contains_match_norm(df["Strategy_Norm"], s)
            df = df[mask]

        # 섹터: OR 조건
        sectors = filters.get("sector") or []
        if sectors:
            mask = pd.Series([False] * len(df), index=df.index)
            for s in sectors:
                mask |= util.contains_match_norm(df["Sector_Norm"], s)
            df = df[mask]

        managers = filters.get("manager") or []
        if managers:
            manager_keywords = self._expand_manager_keywords(managers)
            logging.info("manager filter | raw=%s | expanded=%s", managers, manager_keywords)

            not_blank_mask = df["Manager"].fillna("").astype(str).str.strip() != ""
            mask = pd.Series([False] * len(df), index=df.index)

            for kw in manager_keywords:
                mask |= util.contains_match_norm(df["Manager_Norm"], kw)

            df = df[not_blank_mask & mask]

        project_ids = filters.get("project_id") or []
        if project_ids:
            norm_ids = [util.normalize_text(x) for x in project_ids if str(x).strip()]
            df = df[df["Project_ID_Norm"].isin(norm_ids)]

        # 펀드명 검색 = Asset_Name 기준
        fund_name_keywords = filters.get("fund_name_keywords") or []
        if fund_name_keywords:
            mask = pd.Series([False] * len(df), index=df.index)
            for kw in fund_name_keywords:
                mask |= util.contains_match_norm(df["Asset_Name_Norm"], kw)
            df = df[mask]

        # 자산명 키워드도 현재는 Asset_Name 기준 보조 검색
        asset_name_keywords = filters.get("asset_name_keywords") or []
        if asset_name_keywords:
            mask = pd.Series([False] * len(df), index=df.index)
            for kw in asset_name_keywords:
                mask |= util.contains_match_norm(df["Asset_Name_Norm"], kw)
            df = df[mask]

        vintage_from = filters.get("vintage_from")
        vintage_to = filters.get("vintage_to")
        if vintage_from is not None:
            df = df[df["Vintage"] >= float(vintage_from)]
        if vintage_to is not None:
            df = df[df["Vintage"] <= float(vintage_to)]

        maturity_year_from = filters.get("maturity_year_from")
        maturity_year_to = filters.get("maturity_year_to")
        if maturity_year_from is not None:
            df = df[df["Maturity_Year"] >= float(maturity_year_from)]
        if maturity_year_to is not None:
            df = df[df["Maturity_Year"] <= float(maturity_year_to)]

        irr_min = filters.get("irr_min")
        irr_max = filters.get("irr_max")
        if irr_min is not None:
            df = df[df["IRR"] >= float(irr_min)]
        if irr_max is not None:
            df = df[df["IRR"] <= float(irr_max)]

        commit_min = filters.get("commit_min")
        commit_max = filters.get("commit_max")
        if commit_min is not None:
            df = df[df["Commitment"] >= float(commit_min)]
        if commit_max is not None:
            df = df[df["Commitment"] <= float(commit_max)]

        called_min = filters.get("called_min")
        called_max = filters.get("called_max")
        if called_min is not None:
            df = df[df["Called"] >= float(called_min)]
        if called_max is not None:
            df = df[df["Called"] <= float(called_max)]

        outstanding_min = filters.get("outstanding_min")
        outstanding_max = filters.get("outstanding_max")
        if outstanding_min is not None:
            df = df[df["Outstanding"] >= float(outstanding_min)]
        if outstanding_max is not None:
            df = df[df["Outstanding"] <= float(outstanding_max)]

        nav_min = filters.get("nav_min")
        nav_max = filters.get("nav_max")
        if nav_min is not None:
            df = df[df["NAV"] >= float(nav_min)]
        if nav_max is not None:
            df = df[df["NAV"] <= float(nav_max)]

        df = self._exclude_matured(df)
        return df

    def _project_level_df(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=[
                "Project_ID", "Asset_Name", "Manager", "Asset_Class_Std", "Region_Std",
                "Strategy", "Sector", "Vintage", "Maturity_Year",
                "Commitment", "Called", "Outstanding", "NAV", "IRR"
            ])

        project_df = (
            df
            .groupby("Project_ID", as_index=False)
            .agg({
                "Asset_Name": "first",
                "Manager": "first",
                "Asset_Class_Std": "first",
                "Region_Std": "first",
                "Strategy": "first",
                "Sector": "first",
                "Vintage": "first",
                "Maturity_Year": "min",
                "Commitment": "sum",
                "Called": "sum",
                "Outstanding": "sum",
                "NAV": "sum",
                "IRR": "mean",
            })
            .copy()
        )
        return project_df

    def search(self, query_json: Dict[str, Any]) -> Dict[str, Any]:
        filters = query_json.get("filters", {}) or {}
        filtered_df = self._apply_filters(self.df, filters)
        project_df = self._project_level_df(filtered_df)

        sort_by = (query_json.get("sort", {}) or {}).get("by")
        sort_order = (query_json.get("sort", {}) or {}).get("order", "desc")
        sort_col = {
            "irr": "IRR",
            "commitment": "Commitment",
            "called": "Called",
            "outstanding": "Outstanding",
            "nav": "NAV",
            "maturity_year": "Maturity_Year"
        }.get(sort_by)

        if sort_col and sort_col in project_df.columns:
            ascending = (sort_order == "asc")
            project_df = project_df.sort_values(sort_col, ascending=ascending, na_position="last")
        else:
            project_df = project_df.sort_values(
                ["Commitment", "Project_ID"],
                ascending=[False, True],
                na_position="last"
            )

        limit = int(query_json.get("output", {}).get("limit", config.DEFAULT_LIMIT) or config.DEFAULT_LIMIT)
        limit = max(1, min(limit, config.MAX_LIMIT))
        rows_df = project_df.head(limit).copy()

        rows = []
        for _, row in rows_df.iterrows():
            rows.append({
                "Project_ID": row["Project_ID"],
                "Asset_Name": row["Asset_Name"],
                "Manager": row["Manager"] if row["Manager"] else None,
                "Asset_Class": row["Asset_Class_Std"],
                "Region": row["Region_Std"],
                "Strategy": row["Strategy"] if row["Strategy"] else None,
                "Sector": row["Sector"] if row["Sector"] else None,
                "Vintage": util.safe_num(row["Vintage"]),
                "Maturity_Year": util.safe_num(row["Maturity_Year"]),
                "Commitment": util.safe_num(row["Commitment"]),
                "Called": util.safe_num(row["Called"]),
                "Outstanding": util.safe_num(row["Outstanding"]),
                "NAV": util.safe_num(row["NAV"]),
                "IRR": util.safe_num(row["IRR"]),
            })

        valid_irr_df = project_df.dropna(subset=["IRR", "Commitment"])
        if not valid_irr_df.empty and valid_irr_df["Commitment"].sum() > 0:
            weighted_irr = (
                (valid_irr_df["IRR"] * valid_irr_df["Commitment"]).sum()
                / valid_irr_df["Commitment"].sum()
            )
        else:
            weighted_irr = None

        summary = {
            "count_raw_rows": int(len(filtered_df)),
            "count_projects_total": int(project_df["Project_ID"].nunique()) if not project_df.empty else 0,
            "sum_commitment": util.safe_num(project_df["Commitment"].sum()) if not project_df.empty else 0.0,
            "sum_called": util.safe_num(project_df["Called"].sum()) if not project_df.empty else 0.0,
            "sum_outstanding": util.safe_num(project_df["Outstanding"].sum()) if not project_df.empty else 0.0,
            "sum_nav": util.safe_num(project_df["NAV"].sum()) if not project_df.empty else 0.0,
            "avg_irr": util.safe_num(weighted_irr),
        }

        return {
            "result_type": "search",
            "query_json": query_json,
            "summary": summary,
            "rows": rows,
            "detail_project_ids": rows_df["Project_ID"].tolist() if not rows_df.empty else []
        }

    def analyze(self, analysis_json: Dict[str, Any]) -> Dict[str, Any]:
        analysis_type = analysis_json.get("analysis_type")
        if analysis_type == "share":
            return self._analyze_share(analysis_json)
        if analysis_type == "grouped_metric":
            return self._analyze_grouped_metric(analysis_json)
        raise ValueError(f"지원하지 않는 analysis_type: {analysis_type}")

    def _metric_value(self, project_df: pd.DataFrame, metric: str) -> Optional[float]:
        if project_df.empty:
            return 0.0

        if metric == "count":
            return float(project_df["Project_ID"].nunique())

        if metric == "commitment":
            return util.safe_num(project_df["Commitment"].sum()) or 0.0
        if metric == "called":
            return util.safe_num(project_df["Called"].sum()) or 0.0
        if metric == "outstanding":
            return util.safe_num(project_df["Outstanding"].sum()) or 0.0
        if metric == "nav":
            return util.safe_num(project_df["NAV"].sum()) or 0.0

        valid = project_df.dropna(subset=["IRR"]).copy()
        if valid.empty:
            return None

        if metric == "irr_avg":
            return util.safe_num(valid["IRR"].mean())

        if metric == "irr_weighted_commitment":
            valid = valid.dropna(subset=["Commitment"])
            denom = valid["Commitment"].sum()
            if denom > 0:
                return util.safe_num((valid["IRR"] * valid["Commitment"]).sum() / denom)
            return None

        if metric == "irr_weighted_called":
            valid = valid.dropna(subset=["Called"])
            denom = valid["Called"].sum()
            if denom > 0:
                return util.safe_num((valid["IRR"] * valid["Called"]).sum() / denom)
            return None

        if metric == "irr_weighted_outstanding":
            valid = valid.dropna(subset=["Outstanding"])
            denom = valid["Outstanding"].sum()
            if denom > 0:
                return util.safe_num((valid["IRR"] * valid["Outstanding"]).sum() / denom)
            return None

        if metric == "irr_weighted_nav":
            valid = valid.dropna(subset=["NAV"])
            denom = valid["NAV"].sum()
            if denom > 0:
                return util.safe_num((valid["IRR"] * valid["NAV"]).sum() / denom)
            return None

        raise ValueError(f"지원하지 않는 metric: {metric}")

    def _groupby_col(self, groupby: str) -> str:
        mapping = {
            "asset_class": "Asset_Class_Std",
            "region": "Region_Std",
            "strategy": "Strategy",
            "manager": "Manager",
            "sector": "Sector",
            "vintage": "Vintage",
            "maturity_year": "Maturity_Year",
        }
        if groupby not in mapping:
            raise ValueError(f"지원하지 않는 groupby: {groupby}")
        return mapping[groupby]

    def _analyze_share(self, analysis_json: Dict[str, Any]) -> Dict[str, Any]:
        base_filters = analysis_json.get("base_filters", {}) or {}
        target_filters = analysis_json.get("target_filters", {}) or {}
        metric = str(analysis_json.get("metric", "commitment")).strip().lower()

        base_df = self._apply_filters(self.df, base_filters)
        base_project_df = self._project_level_df(base_df)

        target_df = self._apply_filters(base_df, target_filters)
        target_project_df = self._project_level_df(target_df)

        base_value = self._metric_value(base_project_df, metric)
        target_value = self._metric_value(target_project_df, metric)

        ratio = None
        if base_value is not None and base_value != 0 and target_value is not None:
            ratio = target_value / base_value

        return {
            "result_type": "analysis",
            "analysis_type": "share",
            "analysis_json": analysis_json,
            "metric": metric,
            "base_value": base_value,
            "target_value": target_value,
            "ratio": ratio,
            "base_project_count": int(base_project_df["Project_ID"].nunique()) if not base_project_df.empty else 0,
            "target_project_count": int(target_project_df["Project_ID"].nunique()) if not target_project_df.empty else 0,
        }

    def _analyze_grouped_metric(self, analysis_json: Dict[str, Any]) -> Dict[str, Any]:
        base_filters = analysis_json.get("base_filters", {}) or {}
        groupby = analysis_json.get("groupby", []) or []
        metrics = analysis_json.get("metrics", []) or ["commitment"]
        sort_by = analysis_json.get("sort_by", metrics[0])
        top_n = analysis_json.get("top_n", 50)
        sort_order = analysis_json.get("sort_order", "desc")

        base_df = self._apply_filters(self.df, base_filters)
        base_project_df = self._project_level_df(base_df)

        if base_project_df.empty:
            return {
                "result_type": "analysis",
                "analysis_type": "grouped_metric",
                "rows": [],
                "groupby": groupby,
                "metrics": metrics,
                "base_project_count": 0,
            }

        group_cols = [self._groupby_col(g) for g in groupby]
        rows = []

        grouped = base_project_df.groupby(group_cols, dropna=False)

        for keys, grp in grouped:
            if not isinstance(keys, tuple):
                keys = (keys,)

            group_labels = []
            for val in keys:
                if pd.isna(val) or str(val).strip() == "":
                    group_labels.append("N/A")
                else:
                    group_labels.append(str(val))

            row = {
                "group": group_labels,
                "project_count": int(grp["Project_ID"].nunique())
            }

            for m in metrics:
                row[m] = self._metric_value(grp, m)

            rows.append(row)

        reverse = (sort_order != "asc")
        rows = [r for r in rows if r.get(sort_by) is not None]
        rows.sort(key=lambda x: x.get(sort_by, -999999999), reverse=reverse)
        rows = rows[:top_n]

        return {
            "result_type": "analysis",
            "analysis_type": "grouped_metric",
            "groupby": groupby,
            "metrics": metrics,
            "sort_by": sort_by,
            "rows": rows,
            "base_project_count": int(base_project_df["Project_ID"].nunique())
        }


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

def run_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), Handler)
    server.serve_forever()

# =========================================================
# Gemini 질문 해석
# =========================================================
def build_fixed_query_advice() -> str:
    return (
        "[안내]\n"
        "이 질문은 바로 조회형으로 처리하기 어렵습니다.\n"
        "자산군, 지역, 전략, 운용사, 펀드명, 만기, 수익률, NAV, 콜금액 기준을 포함해 다시 질문해 주세요.\n\n"
        "[예시 조회]\n"
        "- /조회 미국 PD 펀드 중 IRR 높은 상위 5개\n"
        "- /조회 유럽 인프라 펀드 중 27년 이전 만기 건\n"
        "- /조회 블랙스톤 부동산 펀드\n"
        "- /조회 NAV 큰 순 상위 10개"
    )


def build_fixed_analysis_advice() -> str:
    return (
        "[안내]\n"
        "이 질문은 바로 분석형으로 처리하기 어렵습니다.\n"
        "비중, 평균 수익률, 자산군별/전략별/지역별 분석처럼 계산 기준이 드러나도록 다시 질문해 주세요.\n\n"
        "[예시 분석]\n"
        "- /분석 전체 포트폴리오에서 미국 비중\n"
        "- /분석 미국 부동산 투자 중 Core 전략 비중\n"
        "- /분석 자산군별 평균 IRR\n"
        "- /분석 미국 부동산 전략별 평균 IRR"
    )


def _normalize_filter_dict(filters: Dict[str, Any]) -> Dict[str, Any]:
    normalized_filters: Dict[str, Any] = {}

    def norm_str_list(val: Any) -> List[str]:
        if not isinstance(val, list):
            return []
        cleaned = []
        for x in val:
            s = str(x).strip()
            if s:
                cleaned.append(s)
        return cleaned

    asset_classes = [x for x in norm_str_list(filters.get("asset_class")) if x in ASSET_CLASS_ALLOWED]
    if asset_classes:
        normalized_filters["asset_class"] = asset_classes

    regions = [x for x in norm_str_list(filters.get("region")) if x in REGION_ALLOWED]
    if regions:
        normalized_filters["region"] = regions

    for key in ["manager", "strategy", "sector", "project_id", "fund_name_keywords", "asset_name_keywords"]:
        vals = norm_str_list(filters.get(key))
        if vals:
            normalized_filters[key] = vals[:10]

    int_keys = ["vintage_from", "vintage_to", "maturity_year_from", "maturity_year_to"]
    for key in int_keys:
        val = filters.get(key)
        if val is not None:
            try:
                normalized_filters[key] = int(val)
            except Exception:
                pass

    num_keys = [
        "irr_min", "irr_max",
        "commit_min", "commit_max",
        "called_min", "called_max",
        "outstanding_min", "outstanding_max",
        "nav_min", "nav_max"
    ]
    for key in num_keys:
        val = filters.get(key)
        if val is not None:
            try:
                normalized_filters[key] = float(val)
            except Exception:
                pass

    return normalized_filters


def normalize_query_json(query_json: Dict[str, Any]) -> Dict[str, Any]:
    out = {
        "query_type": "summary_with_list",
        "filters": {},
        "sort": {},
        "output": {
            "include_summary": True,
            "include_list": True,
            "limit": config.DEFAULT_LIMIT
        }
    }

    if not isinstance(query_json, dict):
        return out

    out["filters"] = _normalize_filter_dict(query_json.get("filters", {}) or {})

    sort = query_json.get("sort", {}) or {}
    sort_by = str(sort.get("by", "")).strip()
    sort_order = str(sort.get("order", "")).strip().lower()
    if sort_by in SORT_BY_ALLOWED and sort_order in SORT_ORDER_ALLOWED:
        out["sort"] = {"by": sort_by, "order": sort_order}

    output = query_json.get("output", {}) or {}
    limit = output.get("limit", config.DEFAULT_LIMIT)
    try:
        limit = int(limit)
    except Exception:
        limit = config.DEFAULT_LIMIT
    out["output"]["limit"] = max(1, min(limit, config.MAX_LIMIT))

    return out


def normalize_analysis_json(analysis_json: Dict[str, Any]) -> Dict[str, Any]:
    out = {
        "analysis_type": "share",
        "base_filters": {},
        "target_filters": {},
        "metric": "commitment",
        "groupby": [],
        "metrics": ["commitment"],
        "sort_by": "commitment",
        "top_n": 50,
        "sort_order": "desc",
    }

    if not isinstance(analysis_json, dict):
        return out

    analysis_type = str(analysis_json.get("analysis_type", "share")).strip()
    if analysis_type in ANALYSIS_TYPE_ALLOWED:
        out["analysis_type"] = analysis_type

    out["base_filters"] = _normalize_filter_dict(analysis_json.get("base_filters", {}) or {})
    out["target_filters"] = _normalize_filter_dict(analysis_json.get("target_filters", {}) or {})

    metric = analysis_json.get("metric")
    if metric in ANALYSIS_METRIC_ALLOWED:
        out["metric"] = metric

    groupby = analysis_json.get("groupby") or []
    if isinstance(groupby, list):
        groupby = [g for g in groupby if g in GROUPBY_ALLOWED][:2]
    else:
        groupby = [groupby] if groupby in GROUPBY_ALLOWED else []
    out["groupby"] = groupby

    metrics = analysis_json.get("metrics") or []
    if isinstance(metrics, list):
        metrics = [m for m in metrics if m in ANALYSIS_METRIC_ALLOWED][:2]
    else:
        metrics = [metrics] if metrics in ANALYSIS_METRIC_ALLOWED else ["commitment"]
    if not metrics:
        metrics = ["commitment"]
    out["metrics"] = metrics

    sort_by = analysis_json.get("sort_by")
    if sort_by in metrics:
        out["sort_by"] = sort_by
    else:
        out["sort_by"] = metrics[0]

    try:
        out["top_n"] = max(1, min(int(analysis_json.get("top_n", 50)), 100))
    except Exception:
        out["top_n"] = 50

    sort_order = str(analysis_json.get("sort_order", "desc")).lower()
    if sort_order in SORT_ORDER_ALLOWED:
        out["sort_order"] = sort_order

    if out["analysis_type"] == "share":
        out["groupby"] = []
        out["metrics"] = []
        out["sort_by"] = ""
        out["top_n"] = 50
        out["sort_order"] = "desc"

    return out


def is_unprocessable_query(query_json: Dict[str, Any]) -> bool:
    filters = query_json.get("filters", {}) or {}
    has_filter = any(v not in (None, [], {}, "") for v in filters.values())

    sort = query_json.get("sort", {}) or {}
    has_sort = bool(sort.get("by"))

    limit = int(query_json.get("output", {}).get("limit", config.DEFAULT_LIMIT) or config.DEFAULT_LIMIT)
    has_nondefault_limit = (limit != config.DEFAULT_LIMIT)

    if has_filter:
        return False
    if has_sort and has_nondefault_limit:
        return False
    return True


def is_unprocessable_analysis(analysis_json: Dict[str, Any]) -> bool:
    analysis_type = analysis_json.get("analysis_type")

    if analysis_type == "share":
        target_filters = analysis_json.get("target_filters", {}) or {}
        has_target = any(v not in (None, [], {}, "") for v in target_filters.values())
        return not has_target

    if analysis_type == "grouped_metric":
        groupby = analysis_json.get("groupby") or []
        metrics = analysis_json.get("metrics") or []
        return (not bool(groupby)) or (not bool(metrics))

    return True


def parse_question_with_gemini(user_question: str) -> Dict[str, Any]:
    if not USE_GEMINI or client is None:
        return {
            "mode": "advice",
            "query_json": None,
            "advice_text": build_fixed_query_advice()
        }

    prompt = f"""
당신은 투자 포트폴리오 조회 시스템의 질의 해석기입니다.

당신의 역할은 사용자 질문을 아래 두 가지 중 하나로만 변환하는 것입니다.

1) 조회 가능한 경우:
- mode = "query"
- query_json 생성

2) 조회형으로 바로 변환하기 어려운 경우:
- mode = "advice"
- advice_text 생성
- 이 경우 query_json은 null

반드시 JSON만 출력하세요.
설명문, 주석, 마크다운, 코드블록 없이 순수 JSON만 출력하세요.

[시스템에서 조회 가능한 조건]
- 자산군(asset_class): Real_Estate, PE, VC, PD, Infrastructure
- 지역(region): US, Europe, Asia, Global, KOR, MENA, Canada
- 운용사(manager): Manager 컬럼 기준
- 전략(strategy): Strategy 컬럼 기준 자유 텍스트 매칭
- 섹터(sector): Sector 컬럼 기준 자유 텍스트 매칭
- 프로젝트ID(project_id): Project_ID
- 펀드명(fund_name_keywords): Asset_Name 컬럼 기준 키워드 검색
- 자산명(asset_name_keywords): Asset_Name 컬럼 기준 보조 검색
- 빈티지(vintage_from, vintage_to): Vintage 기준
- 만기(maturity_year_from, maturity_year_to): Maturity_Year 기준
- 수익률(irr_min, irr_max): IRR 기준
- 약정금액(commit_min, commit_max): Commitment 기준
- 콜금액(called_min, called_max): Called 기준
- 투자잔액(outstanding_min, outstanding_max): 투자잔액 기준
- NAV(nav_min, nav_max): NAV 기준
- 정렬(sort.by, sort.order):
  - irr asc/desc
  - commitment asc/desc
  - called asc/desc
  - outstanding asc/desc
  - nav asc/desc
  - maturity_year asc/desc
- 개수 제한(output.limit)

[추가 정보]
- 국내 부동산도 데이터에 포함되어 있습니다.
- 국내 부동산은 Asset_Class=Real_Estate, Region=KOR 입니다.
- 국내 부동산 전략 예시: PF대출, 담보대출
- 국내 부동산의 IRR 컬럼은 All-In 금리이므로 수익률/IRR 조회 시 동일하게 사용합니다.
- "해외"라고 하면 Region=KOR을 제외한 전 지역을 의미합니다.
  즉 region은 ["US", "Europe", "Asia", "Global", "MENA", "Canada"] 로 해석하세요.
- 펀드명은 Asset_Name 컬럼으로 검색합니다.

[중요 규칙]
1. 사용자가 명확한 조회 조건을 말하면 mode="query"로 반환하세요.
2. 추상적/판단형 질문이면 mode="advice"로 반환하세요.
3. mode="advice"일 때는 query_json을 만들지 마세요.
4. advice_text는 짧고 실무적으로 작성하세요.
5. query_json 구조는 아래 형식만 허용합니다.

{QUERY_SCHEMA_PROMPT}

6. 필터에 없는 조건은 넣지 마세요.
7. 불필요한 빈 배열, null, 빈 문자열은 넣지 마세요.
8. "상위 5개", "top 10", "10개 보여줘" 같은 표현은 output.limit에 반영하세요.
9. "가장 높은", "최고", "최저" 같은 최상급 표현이 있고 숫자 limit가 없으면 output.limit=1로 설정하세요.
10. "수익률 높은 순", "성과 좋은 순" 등은 sort.by="irr", sort.order="desc"
11. "수익률 낮은 순"은 sort.by="irr", sort.order="asc"
12. "약정 큰 순"은 sort.by="commitment", sort.order="desc"
13. "약정 작은 순"은 sort.by="commitment", sort.order="asc"
14. "콜금액 큰 순"은 sort.by="called", sort.order="desc"
15. "콜금액 작은 순"은 sort.by="called", sort.order="asc"
16. "투자잔액 큰 순"은 sort.by="outstanding", sort.order="desc"
17. "투자잔액 작은 순"은 sort.by="outstanding", sort.order="asc"
18. "NAV 큰 순"은 sort.by="nav", sort.order="desc"
19. "NAV 작은 순"은 sort.by="nav", sort.order="asc"
20. "만기 빠른 순"은 sort.by="maturity_year", sort.order="asc"
21. "만기 느린 순"은 sort.by="maturity_year", sort.order="desc"
22. 부동산 전략은 복합 문자열일 수 있습니다.
예:
- Senior|Core
- Equity|Core
- Mezzanine|Core
- Mezzanine|Value-add
- Senior|Value-add
- Senior|Opportunistic
- Senior|Core plus
- Opportunistic
- PF대출
- 담보대출

23. 따라서 strategy에는 아래 표현을 넣을 수 있습니다.
- senior / 시니어
- equity / 에쿼티 / 에퀴티
- mezzanine / 메자닌 / mezz
- core / 코어
- core plus / 코어플러스 / coreplus
- value-add / 밸류애드 / valueadd
- opportunistic / 오퍼튜니스틱
- pf대출
- 담보대출

24. 전략이 여러 개이면 AND 조건으로 검색됩니다.
25. 섹터가 여러 개이면 OR 조건으로 검색됩니다.
26. 사용자가 운용사명을 말한 경우에는 manager만 사용하세요.
27. fund_name_keywords는 특정 펀드 고유명칭이 명확할 때만 사용하세요.
28. 운용사명과 fund_name_keywords를 같은 의미로 중복 넣지 마세요.
29. asset_name_keywords는 펀드명 조건이 불명확할 때만 보조적으로 사용하세요.
30. 전략과 섹터는 영어로 입력되어 있으니, 한글은 가능한 영문 키워드로 변환하세요.
31. 인프라에서 비민자 사업의 전략은 Non-PPP 입니다. 민자사업은 전략이 BT로 시작됩니다.


[예시 1]
사용자 질문:
미국 부동산 투자 펀드 중 Core 전략 펀드

응답:
{{
  "mode": "query",
  "query_json": {{
    "query_type": "summary_with_list",
    "filters": {{
      "asset_class": ["Real_Estate"],
      "region": ["US"],
      "strategy": ["core"]
    }},
    "sort": {{}},
    "output": {{
      "include_summary": true,
      "include_list": true,
      "limit": 9999
    }}
  }},
  "advice_text": null
}}

[예시 2]
사용자 질문:
해외 부동산 Core Senior 전략

응답:
{{
  "mode": "query",
  "query_json": {{
    "query_type": "summary_with_list",
    "filters": {{
      "asset_class": ["Real_Estate"],
      "region": ["US", "Europe", "Asia", "Global", "MENA", "Canada"],
      "strategy": ["core", "senior"]
    }},
    "sort": {{}},
    "output": {{
      "include_summary": true,
      "include_list": true,
      "limit": 9999
    }}
  }},
  "advice_text": null
}}

[예시 3]
사용자 질문:
블랙락 유럽 부동산 펀드

응답:
{{
  "mode": "query",
  "query_json": {{
    "query_type": "summary_with_list",
    "filters": {{
      "manager": ["BlackRock"],
      "region": ["Europe"],
      "asset_class": ["Real_Estate"]
    }},
    "sort": {{}},
    "output": {{
      "include_summary": true,
      "include_list": true,
      "limit": 9999
    }}
  }},
  "advice_text": null
}}

[예시 4]
사용자 질문:
수익률 높은 상위 5개

응답:
{{
  "mode": "query",
  "query_json": {{
    "query_type": "summary_with_list",
    "filters": {{}},
    "sort": {{
      "by": "irr",
      "order": "desc"
    }},
    "output": {{
      "include_summary": true,
      "include_list": true,
      "limit": 5
    }}
  }},
  "advice_text": null
}}

[예시 5]
사용자 질문:
요즘 시장에서 괜찮은 전략 뭐야

응답:
{{
  "mode": "advice",
  "query_json": null,
  "advice_text": "[안내]\\n이 질문은 판단형 질문이라 바로 조회 조건으로 바꾸기 어렵습니다.\\n\\n[예시]\\n- /조회 미국 PD 펀드 중 IRR 높은 상위 5개\\n- /조회 유럽 인프라 펀드 중 27년 이전 만기 건\\n- /조회 블랙스톤 부동산 펀드"
}}

사용자 질문:
{user_question}
""".strip()

    try:
        resp = client.models.generate_content(
            model=config.GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0.1,
                max_output_tokens=1600,
                response_mime_type="application/json",
            ),
        )
        text = (resp.text or "").strip()
        if not text:
            raise ValueError("Gemini 응답이 비어 있습니다.")

        data = json.loads(text)

        mode = str(data.get("mode", "")).strip().lower()
        if mode == "query":
            query_json = normalize_query_json(data.get("query_json") or {})
            if is_unprocessable_query(query_json):
                return {
                    "mode": "advice",
                    "query_json": None,
                    "advice_text": build_fixed_query_advice()
                }
            return {
                "mode": "query",
                "query_json": query_json,
                "advice_text": None
            }

        advice_text = str(data.get("advice_text") or "").strip()
        if not advice_text:
            advice_text = build_fixed_query_advice()

        return {
            "mode": "advice",
            "query_json": None,
            "advice_text": advice_text
        }

    except Exception:
        logging.exception("Gemini question parsing failed")
        return {
            "mode": "advice",
            "query_json": None,
            "advice_text": build_fixed_query_advice()
        }


def parse_analysis_with_gemini(user_question: str) -> Dict[str, Any]:
    if not USE_GEMINI or client is None:
        return {
            "mode": "advice",
            "analysis_json": None,
            "advice_text": build_fixed_analysis_advice()
        }

    prompt = f"""
당신은 투자 포트폴리오 분석 시스템의 질의 해석기입니다.

당신의 역할은 사용자 질문을 아래 두 가지 중 하나로만 변환하는 것입니다.

1) 분석 가능한 경우:
- mode = "analysis"
- analysis_json 생성

2) 분석형으로 바로 변환하기 어려운 경우:
- mode = "advice"
- advice_text 생성
- 이 경우 analysis_json은 null

반드시 JSON만 출력하세요.
설명문, 주석, 마크다운, 코드블록 없이 순수 JSON만 출력하세요.

[분석 가능한 유형]
1. share
- 전체 대비 특정 부분집합 비중 분석
- 예: 전체 포트폴리오에서 미국 비중
- 예: 미국 부동산 투자 중 Core 전략 비중
- 예: PE 자산군에서 KKR 약정 비중

2. grouped_metric
- 특정 모집단을 그룹별로 나누어 metric 집계
- 예: 자산군별 평균 IRR
- 예: 전략별 평균 IRR
- 예: 지역별 NAV
- 예: 자산군별 약정액
- 예: 미국 부동산 전략별 평균 IRR

[분석 기준]
- base_filters: 분석의 모집단
- target_filters: share 분석의 부분집합
- metric: share 분석에서 사용하는 단일 metric
- groupby: grouped_metric 분석 시 그룹 기준(최대 2개)
- metrics: grouped_metric 분석 시 metric 목록(최대 2개)
- sort_by: grouped_metric 정렬 기준

[metric 후보]
- commitment
- called
- outstanding
- nav
- count
- irr_avg
- irr_weighted_commitment
- irr_weighted_called
- irr_weighted_outstanding
- irr_weighted_nav

[중요 해석 규칙]
1. "비중"은 share 로 해석하세요.
2. "평균 수익률", "평균 IRR"은 grouped_metric 으로 해석하세요.
3. "자산군별", "전략별", "지역별", "운용사별", "섹터별", "빈티지별", "만기별"은 grouped_metric 으로 해석하세요.
4. "전체 포트폴리오"는 base_filters={{}} 입니다.
5. "해외"라고 하면 Region=KOR을 제외한 전 지역을 의미합니다.
   즉 region은 ["US", "Europe", "Asia", "Global", "MENA", "Canada"] 로 해석하세요.
6. 사용자가 metric을 명시하지 않으면:
   - 비중 분석은 commitment 기준
   - 평균 수익률 분석은 irr_weighted_commitment 기준
   - 금액 집계는 질문에 따라 commitment / called / outstanding / nav 선택
7. "NAV 기준", "잔액 기준", "아웃스탠딩 기준", "건수 기준", "콜금액 기준" 같은 표현이 있으면 해당 metric을 사용하세요.
8. grouped_metric 에서 top_n이 명시되지 않으면 기본 20으로 두세요.
9. grouped_metric 에서 sort_order가 명시되지 않으면 desc 로 두세요.
10. strategy/sector 한글 표현은 가능한 한 영문 키워드로 변환하세요.
11. 만기가 지난 투자건은 제외하세요.
12. 인프라에서 비민자 사업의 전략은 Non-PPP 입니다. 민자사업은 전략이 BT로 시작됩니다.
13. grouped_metric 에서는 groupby는 배열로 넣으세요. 예: ["asset_class"], ["region","strategy"]
14. grouped_metric 에서는 metrics도 배열로 넣으세요. 예: ["irr_weighted_commitment"], ["commitment","count"]
15. 전략이 여러 개이면 AND 조건으로 해석하세요.
16. 섹터가 여러 개이면 OR 조건으로 해석하세요.
17. 펀드명은 Asset_Name 컬럼으로 검색하므로, 특정 펀드를 지칭하는 경우 fund_name_keywords를 활용할 수 있습니다.

[매우 중요한 규칙: "A에서 B 비중" 해석]
아래와 같은 표현은 반드시 share 분석으로 해석하세요.

- "A에서 B 비중"
- "A 내 B 비중"
- "A 중 B 비중"
- "A 포트폴리오에서 B 비중"
- "A 자산군에서 B 비중"

이 경우:
- A = 모집단(base_filters)
- B = 모집단 내부의 대상(target_filters)

예:
- "PE 자산군에서 KKR 약정 비중"
  -> base_filters={{"asset_class": ["PE"]}}
  -> target_filters={{"manager": ["KKR"]}}
  -> metric="commitment"

- "미국 부동산에서 Core 비중"
  -> base_filters={{"asset_class": ["Real_Estate"], "region": ["US"]}}
  -> target_filters={{"strategy": ["core"]}}
  -> metric="commitment"

[grouped_metric 규칙]
- grouped_metric 분석에서는 base_filters로 모집단을 먼저 제한하고, 그 내부를 groupby 기준으로 나눕니다.
- 예:
  - "자산군별 평균 IRR"
    -> base_filters={{}}, groupby=["asset_class"], metrics=["irr_weighted_commitment"], sort_by="irr_weighted_commitment"
  - "미국 부동산 전략별 평균 IRR"
    -> base_filters={{"asset_class":["Real_Estate"], "region":["US"]}}, groupby=["strategy"], metrics=["irr_weighted_commitment"], sort_by="irr_weighted_commitment"
  - "해외 부동산 지역별 NAV"
    -> base_filters={{"asset_class":["Real_Estate"], "region":["US", "Europe", "Asia", "Global", "MENA", "Canada"]}}, groupby=["region"], metrics=["nav"], sort_by="nav"

analysis_json 구조는 아래 형식만 허용합니다.

[운용사 해석 규칙]
- 사용자가 운용사명 또는 운용사 그룹명(예: BlackRock, 블랙락, HPS, GIP, TPG, Angelo Gordon, Apogem, GoldPoint)을 말하면 manager 필터로 해석하세요.
- 특정 운용사의 비중을 묻는 경우 share 분석으로 해석하세요.
- 특정 운용사 그룹명을 묻더라도 manager에 그 이름 그대로 넣으세요. 실제 그룹 확장은 시스템이 후처리합니다.
- 예: "PE 자산군에서 블랙락 비중" -> target_filters={{"manager":["BlackRock"]}}
- 예: "PD 자산군에서 HPS 비중" -> target_filters={{"manager":["HPS"]}}
- 예: "PE에서 Angelo Gordon 비중" -> target_filters={{"manager":["Angelo Gordon"]}}


{ANALYSIS_SCHEMA_PROMPT}

[중요 출력 규칙]
1. filters에 없는 조건은 넣지 마세요.
2. 불필요한 빈 배열, null, 빈 문자열은 넣지 마세요.
3. mode="advice"일 때는 analysis_json을 만들지 마세요.
4. 애매한 경우 억지로 분석 JSON을 만들지 말고 advice로 보내세요.
5. 반드시 JSON 형식만 출력하세요.

[예시 1]
사용자 질문:
전체 포트폴리오에서 미국에 투자한 비중이 얼마야

응답:
{{
  "mode": "analysis",
  "analysis_json": {{
    "analysis_type": "share",
    "base_filters": {{}},
    "target_filters": {{
      "region": ["US"]
    }},
    "metric": "commitment"
  }},
  "advice_text": null
}}

[예시 2]
사용자 질문:
미국 부동산 투자 중 Core 전략 비중

응답:
{{
  "mode": "analysis",
  "analysis_json": {{
    "analysis_type": "share",
    "base_filters": {{
      "asset_class": ["Real_Estate"],
      "region": ["US"]
    }},
    "target_filters": {{
      "strategy": ["core"]
    }},
    "metric": "commitment"
  }},
  "advice_text": null
}}

[예시 3]
사용자 질문:
PE 자산군에서 KKR 약정 비중

응답:
{{
  "mode": "analysis",
  "analysis_json": {{
    "analysis_type": "share",
    "base_filters": {{
      "asset_class": ["PE"]
    }},
    "target_filters": {{
      "manager": ["KKR"]
    }},
    "metric": "commitment"
  }},
  "advice_text": null
}}

[예시 4]
사용자 질문:
자산군별 평균 IRR

응답:
{{
  "mode": "analysis",
  "analysis_json": {{
    "analysis_type": "grouped_metric",
    "base_filters": {{}},
    "groupby": ["asset_class"],
    "metrics": ["irr_weighted_commitment"],
    "sort_by": "irr_weighted_commitment",
    "top_n": 50,
    "sort_order": "desc"
  }},
  "advice_text": null
}}

[예시 5]
사용자 질문:
미국 부동산 전략별 평균 IRR

응답:
{{
  "mode": "analysis",
  "analysis_json": {{
    "analysis_type": "grouped_metric",
    "base_filters": {{
      "asset_class": ["Real_Estate"],
      "region": ["US"]
    }},
    "groupby": ["strategy"],
    "metrics": ["irr_weighted_commitment"],
    "sort_by": "irr_weighted_commitment",
    "top_n": 50,
    "sort_order": "desc"
  }},
  "advice_text": null
}}

[예시 6]
사용자 질문:
해외 부동산 지역별 NAV

응답:
{{
  "mode": "analysis",
  "analysis_json": {{
    "analysis_type": "grouped_metric",
    "base_filters": {{
      "asset_class": ["Real_Estate"],
      "region": ["US", "Europe", "Asia", "Global", "MENA", "Canada"]
    }},
    "groupby": ["region"],
    "metrics": ["nav"],
    "sort_by": "nav",
    "top_n": 50,
    "sort_order": "desc"
  }},
  "advice_text": null
}}

[예시 7]
사용자 질문:
PE 자산군에서 블랙락 비중

응답:
{{
  "mode": "analysis",
  "analysis_json": {{
    "analysis_type": "share",
    "base_filters": {{
      "asset_class": ["PE"]
    }},
    "target_filters": {{
      "manager": ["BlackRock"]
    }},
    "metric": "commitment"
  }},
  "advice_text": null
}}

[예시 8]
사용자 질문:
PD 자산군에서 HPS 비중

응답:
{{
  "mode": "analysis",
  "analysis_json": {{
    "analysis_type": "share",
    "base_filters": {{
      "asset_class": ["PD"]
    }},
    "target_filters": {{
      "manager": ["HPS"]
    }},
    "metric": "commitment"
  }},
  "advice_text": null
}}

[예시 9]
사용자 질문:
PE 운용사별 NAV

응답:
{{
  "mode": "analysis",
  "analysis_json": {{
    "analysis_type": "grouped_metric",
    "base_filters": {{
      "asset_class": ["PE"]
    }},
    "groupby": ["manager"],
    "metrics": ["nav"],
    "sort_by": "nav",
    "top_n": 50,
    "sort_order": "desc"
  }},
  "advice_text": null
}}

사용자 질문:
{user_question}
""".strip()

    try:
        resp = client.models.generate_content(
            model=config.GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0.1,
                max_output_tokens=1600,
                response_mime_type="application/json",
            ),
        )
        text = (resp.text or "").strip()
        if not text:
            raise ValueError("Gemini 응답이 비어 있습니다.")

        data = json.loads(text)

        mode = str(data.get("mode", "")).strip().lower()
        if mode == "analysis":
            analysis_json = normalize_analysis_json(data.get("analysis_json") or {})
            if is_unprocessable_analysis(analysis_json):
                return {
                    "mode": "advice",
                    "analysis_json": None,
                    "advice_text": build_fixed_analysis_advice()
                }
            return {
                "mode": "analysis",
                "analysis_json": analysis_json,
                "advice_text": None
            }

        advice_text = str(data.get("advice_text") or "").strip()
        if not advice_text:
            advice_text = build_fixed_analysis_advice()

        return {
            "mode": "advice",
            "analysis_json": None,
            "advice_text": advice_text
        }

    except Exception:
        logging.exception("Gemini analysis parsing failed")
        return {
            "mode": "advice",
            "analysis_json": None,
            "advice_text": build_fixed_analysis_advice()
        }

# =========================================================
# 해석 설명
# =========================================================
def _humanize_filter_summary(filters: Dict[str, Any]) -> List[str]:
    parts: List[str] = []

    asset_class = filters.get("asset_class") or []
    if asset_class:
        parts.append(f"자산군={','.join(asset_class)}")

    region = filters.get("region") or []
    if region:
        if set(region) == set(OVERSEAS_REGIONS):
            parts.append("지역=해외(KOR 제외)")
        else:
            parts.append(f"지역={','.join(region)}")

    manager = filters.get("manager") or []
    if manager:
        parts.append(f"운용사={','.join(manager)}")

    strategy = filters.get("strategy") or []
    if strategy:
        parts.append(f"전략={'+'.join(strategy)}")

    sector = filters.get("sector") or []
    if sector:
        parts.append(f"섹터={','.join(sector)}")

    project_id = filters.get("project_id") or []
    if project_id:
        parts.append(f"Project_ID={','.join(project_id)}")

    fund_name_keywords = filters.get("fund_name_keywords") or []
    if fund_name_keywords:
        parts.append(f"펀드명키워드={','.join(fund_name_keywords)}")

    asset_name_keywords = filters.get("asset_name_keywords") or []
    if asset_name_keywords:
        parts.append(f"자산명키워드={','.join(asset_name_keywords)}")

    if filters.get("vintage_from") is not None or filters.get("vintage_to") is not None:
        vf = filters.get("vintage_from")
        vt = filters.get("vintage_to")
        if vf is not None and vt is not None:
            parts.append(f"Vintage={vf}~{vt}")
        elif vf is not None:
            parts.append(f"Vintage>={vf}")
        else:
            parts.append(f"Vintage<={vt}")

    if filters.get("maturity_year_from") is not None or filters.get("maturity_year_to") is not None:
        mf = filters.get("maturity_year_from")
        mt = filters.get("maturity_year_to")
        if mf is not None and mt is not None:
            parts.append(f"만기년도={mf}~{mt}")
        elif mf is not None:
            parts.append(f"만기년도>={mf}")
        else:
            parts.append(f"만기년도<={mt}")

    num_fields = [
        ("irr_min", "irr_max", "IRR"),
        ("commit_min", "commit_max", "약정"),
        ("called_min", "called_max", "콜금액"),
        ("outstanding_min", "outstanding_max", "투자잔액"),
        ("nav_min", "nav_max", "NAV"),
    ]
    for min_key, max_key, label in num_fields:
        vmin = filters.get(min_key)
        vmax = filters.get(max_key)
        if vmin is not None or vmax is not None:
            if vmin is not None and vmax is not None:
                parts.append(f"{label}={vmin}~{vmax}")
            elif vmin is not None:
                parts.append(f"{label}>={vmin}")
            else:
                parts.append(f"{label}<={vmax}")

    return parts


def summarize_query_json(query_json: Dict[str, Any]) -> str:
    filters = query_json.get("filters", {}) or {}
    sort = query_json.get("sort", {}) or {}
    limit = int(query_json.get("output", {}).get("limit", config.DEFAULT_LIMIT) or config.DEFAULT_LIMIT)

    parts = _humanize_filter_summary(filters)

    sort_by = sort.get("by")
    sort_order = sort.get("order")
    sort_label_map = {
        "irr": "IRR",
        "commitment": "약정",
        "called": "콜금액",
        "outstanding": "투자잔액",
        "nav": "NAV",
        "maturity_year": "만기년도",
    }

    if sort_by:
        direction = "오름차순" if sort_order == "asc" else "내림차순"
        parts.append(f"정렬={sort_label_map.get(sort_by, sort_by)} {direction}")

    if limit != config.DEFAULT_LIMIT:
        parts.append(f"표시건수={limit}")

    if not parts:
        return "전체 포트폴리오 기준 조회로 이해했습니다."

    return f"{', '.join(parts)} 조건으로 조회했습니다."


def summarize_analysis_json(analysis_json: Dict[str, Any]) -> str:
    analysis_type = analysis_json.get("analysis_type")
    metric = analysis_json.get("metric", "commitment")

    metric_label_map = {
        "commitment": "약정 기준",
        "called": "콜금액 기준",
        "outstanding": "투자잔액 기준",
        "nav": "NAV 기준",
        "count": "건수 기준",
        "irr_avg": "단순평균 IRR 기준",
        "irr_weighted_commitment": "약정가중 평균 IRR 기준",
        "irr_weighted_called": "콜금액가중 평균 IRR 기준",
        "irr_weighted_outstanding": "투자잔액가중 평균 IRR 기준",
        "irr_weighted_nav": "NAV가중 평균 IRR 기준",
    }

    if analysis_type == "share":
        base_parts = _humanize_filter_summary(analysis_json.get("base_filters", {}) or {})
        target_parts = _humanize_filter_summary(analysis_json.get("target_filters", {}) or {})
        base_text = "전체 포트폴리오" if not base_parts else f"모집단({', '.join(base_parts)})"
        target_text = "대상조건" if not target_parts else f"대상({', '.join(target_parts)})"
        return f"{base_text} 대비 {target_text} 비중 분석으로 이해했습니다. ({metric_label_map.get(metric, metric)})"

    if analysis_type == "grouped_metric":
        base_parts = _humanize_filter_summary(analysis_json.get("base_filters", {}) or {})
        base_text = "전체 포트폴리오" if not base_parts else ", ".join(base_parts)

        groupby_list = analysis_json.get("groupby") or []
        metrics_list = analysis_json.get("metrics") or []

        groupby_label_map = {
            "asset_class": "자산군",
            "region": "지역",
            "strategy": "전략",
            "manager": "운용사",
            "sector": "섹터",
            "vintage": "Vintage",
            "maturity_year": "만기년도",
        }

        groupby_text = "/".join([groupby_label_map.get(g, g) for g in groupby_list]) if groupby_list else "그룹"
        metric_text = "/".join([metric_label_map.get(m, m) for m in metrics_list]) if metrics_list else "지표"

        return f"{base_text} 기준 {groupby_text}별 {metric_text} 분석으로 이해했습니다."

    return "포트폴리오 분석 요청으로 이해했습니다."


# =========================================================
# 답변 생성
# =========================================================
def build_search_answer(retrieved: Dict[str, Any], interpretation: str) -> str:
    summary = retrieved["summary"]
    rows = retrieved["rows"]

    lines: List[str] = []
    lines.append("[해석]")
    lines.append(interpretation)
    lines.append("")

    if summary["count_projects_total"] == 0:
        lines.append("[핵심 요약]")
        lines.append("조건에 맞는 투자건이 없습니다.")
        lines.append("")
        lines.append("[리스트]")
        lines.append("-")
        return "\n".join(lines)

    lines.append("[핵심 요약]")
    lines.append(
        f"조건에 맞는 투자건은 총 {summary['count_projects_total']}건입니다. "
        f"총 약정액 {util.format_amount_uk(summary['sum_commitment'])}, "
        f"누적 인출액 {util.format_amount_uk(summary['sum_called'])}, "
        f"현재 투자잔액 {util.format_amount_uk(summary['sum_outstanding'])}, "
        f"NAV는 {util.format_amount_uk(summary['sum_nav'])}입니다."
    )
    lines.append(f"가중평균 IRR은 {util.format_pct(summary['avg_irr'])}입니다.")
    lines.append("")

    lines.append("[리스트]")
    for idx, r in enumerate(rows, start=1):
        extra = []
        if r.get("Manager"):
            extra.append(f"{r['Manager']}")
        if r.get("Region"):
            extra.append(f"{r['Region']}")
        if r.get("Asset_Class"):
            extra.append(f"{r['Asset_Class']}")
        if r.get("Strategy"):
            extra.append(f"{r['Strategy']}")
        if r.get("Sector"):
            extra.append(f"{r['Sector']}")
        if r.get("Vintage") is not None:
            extra.append(f"Vintage: {int(r['Vintage'])}")
        if r.get("Maturity_Year") is not None:
            extra.append(f"만기: {int(r['Maturity_Year'])}")
        if r.get("IRR") is not None:
            extra.append(f"IRR: {util.format_pct(r['IRR'])}")
        if r.get("NAV") is not None:
            extra.append(f"NAV: {util.format_amount_uk(r['NAV'])}")

        tail = f" ({' | '.join(extra)})" if extra else ""
        lines.append(f"{idx}. {r['Project_ID']} | {r['Asset_Name']}{tail}")

    if rows:
        lines.append("")
        lines.append("[상세]")
        lines.append(f"/상세조회 {rows[0]['Project_ID']}")

    return "\n".join(lines)


def build_analysis_answer(retrieved: Dict[str, Any], interpretation: str) -> str:
    analysis_type = retrieved.get("analysis_type")

    lines: List[str] = []
    lines.append("[해석]")
    lines.append(interpretation)
    lines.append("")

    if analysis_type == "share":
        metric = retrieved.get("metric", "commitment")

        metric_label_map = {
            "commitment": "약정액",
            "called": "콜금액",
            "outstanding": "투자잔액",
            "nav": "NAV",
            "count": "건수",
            "irr_avg": "평균 IRR",
            "irr_weighted_commitment": "약정가중 IRR",
            "irr_weighted_called": "콜금액가중 IRR",
            "irr_weighted_outstanding": "잔액가중 IRR",
            "irr_weighted_nav": "NAV가중 IRR",
        }

        base_value = retrieved.get("base_value")
        target_value = retrieved.get("target_value")
        ratio = retrieved.get("ratio")

        lines.append("[핵심 요약]")

        if ratio is None:
            lines.append("비중을 계산할 수 없습니다. (모수 값 없음)")
            return "\n".join(lines)

        lines.append(f"전체 대비 비중: {util.format_pct(ratio)} ({metric_label_map.get(metric, metric)} 기준)")
        lines.append("")
        lines.append("[세부]")

        def _format(v):
            if metric == "count":
                return f"{int(v or 0)}건"
            elif metric.startswith("irr_"):
                return util.format_pct(v)
            else:
                return util.format_amount_uk(v)

        lines.append(f"- 전체: {_format(base_value)} / {retrieved.get('base_project_count', 0)}건")
        lines.append(f"- 대상: {_format(target_value)} / {retrieved.get('target_project_count', 0)}건")
        return "\n".join(lines)

    if analysis_type == "grouped_metric":
        rows = retrieved.get("rows", []) or []
        metrics = retrieved.get("metrics", [])
        groupby = retrieved.get("groupby", [])

        lines.append("[핵심 요약]")

        if not rows:
            lines.append("조건에 맞는 분석 결과가 없습니다.")
            return "\n".join(lines)

        groupby_label_map = {
            "asset_class": "자산군",
            "region": "지역",
            "strategy": "전략",
            "manager": "운용사",
            "sector": "섹터",
            "vintage": "Vintage",
            "maturity_year": "만기",
        }

        metric_label_map = {
            "commitment": "약정",
            "called": "콜금액",
            "outstanding": "잔액",
            "nav": "NAV",
            "count": "건수",
            "irr_avg": "평균IRR",
            "irr_weighted_commitment": "IRR(약정가중)",
            "irr_weighted_called": "IRR(콜가중)",
            "irr_weighted_outstanding": "IRR(잔액가중)",
            "irr_weighted_nav": "IRR(NAV가중)",
        }

        group_header = " | ".join([groupby_label_map.get(g, g) for g in groupby])
        metric_header = " | ".join([metric_label_map.get(m, m) for m in metrics])

        lines.append(f"{group_header} 기준 분석")
        lines.append(f"지표: {metric_header}")
        lines.append("")
        lines.append("[분석 결과]")

        for idx, r in enumerate(rows, start=1):
            group_values = r.get("group", [])
            group_text = " | ".join(group_values)

            metric_parts = []
            for m in metrics:
                val = r.get(m)

                if m == "count":
                    metric_parts.append(f"{int(val or 0)}건")
                elif m.startswith("irr_"):
                    metric_parts.append(util.format_pct(val))
                else:
                    metric_parts.append(util.format_amount_uk(val))

            metric_text = " | ".join(metric_parts)
            lines.append(f"{idx}. {group_text} | {metric_text} | {r.get('project_count', 0)}건")

        total_count = retrieved.get("base_project_count")
        if total_count:
            lines.append("")
            lines.append(f"[총 투자건수] {total_count}건")

        return "\n".join(lines)

    lines.append("[핵심 요약]")
    lines.append("분석 결과를 생성하지 못했습니다.")
    return "\n".join(lines)


# =========================================================
# 명령 처리
# =========================================================
HELP_TEXT = """
한화생명 대체투자 포트폴리오 분석 및 조회 Bot 입니다.
(데이터 기준: 26.2월 / 펀드상세 PDF: 25.9월)

[안내]
- /분석: 비중/평균/그룹별 집계 등 포트폴리오 분석용입니다.
- /조회: 조건에 맞는 건을 조회하고, 필요 시 펀드상세 PDF를 생성할 수 있습니다.
- /검색: 입력한 키워드 관련 뉴스를 요약, 정리하여 볼 수 있습니다.
- AI API 정책 상 조회/분석 수가 제한될 수 있습니다.

[분석 및 조회 가능 내역]
- 자산군, 운용사, 지역, 전략, 섹터, 펀드명, Vintage, 만기, 약정, NAV, 잔액

[사용 법]
/분석 포트폴리오 분석하고 싶은 내용
/조회 조회하고 싶은 내용
/검색 검색 키워드
/help 도움말
/refresh DB Refresh
/상세조회 BS00001505

[분석 예시]
/분석 전체 포트폴리오에서 미국 비중
/분석 미국 부동산 투자 중 Core 전략 비중
/분석 자산군별 평균 IRR
/분석 미국 부동산 전략별 평균 IRR
/분석 해외 부동산 지역별 NAV

[조회 예시]
/조회 KKR에 투자한 PE 펀드 중 2022년 Vintage
/조회 22년 투자 VC 펀드 중 Late Stage 펀드 현황
/조회 미국 지역에 투자한 부동산 펀드 중 26년 만기 도래 건
/조회 해외 부동산 core senior
/조회 블랙스톤 부동산 펀드
""".strip()


def _check_ai_limit_or_reply(chat_id: int, ctx: Dict[str, Any]) -> bool:
    sender_user_id = ctx.get("sender_user_id")
    allowed, _ = util.check_and_increment_question_limit(
        sender_user_id=sender_user_id,
        limit=config.DAILY_QUESTION_LIMIT
    )
    if not allowed:
        display_name = util.get_sender_display_name(ctx)
        send_message(
            chat_id,
            f"{display_name}님은 오늘 조회/분석 한도({config.DAILY_QUESTION_LIMIT}회)를 모두 사용했습니다."
        )
        return False
    return True


def handle_query_command(db: "InvestmentDB", chat_id: int, raw: str, ctx: Dict[str, Any]) -> None:
    question = raw[len("/조회"):].strip()

    if not question:
        send_message(chat_id, "조회할 내용을 입력해 주세요. 예: /조회 미국 부동산 투자 현황")
        return

    if not _check_ai_limit_or_reply(chat_id, ctx):
        return

    try:
        parsed = parse_question_with_gemini(question)
        mode = parsed.get("mode")

        if mode == "advice":
            send_message(chat_id, parsed.get("advice_text") or build_fixed_query_advice())
            return

        query_json = parsed.get("query_json")
        if not query_json:
            send_message(chat_id, build_fixed_query_advice())
            return

        logging.info("query_json=%s", json.dumps(query_json, ensure_ascii=False))
        retrieved = db.search(query_json)
        interpretation = summarize_query_json(query_json)
        answer = build_search_answer(retrieved, interpretation)
        send_message(chat_id, answer)

    except Exception as e:
        logging.exception("query command failed")
        send_message(chat_id, f"조회 처리 중 오류가 발생했습니다.\n{e}")


def handle_analysis_command(db: "InvestmentDB", chat_id: int, raw: str, ctx: Dict[str, Any]) -> None:
    question = raw[len("/분석"):].strip()

    if not question:
        send_message(chat_id, "분석할 내용을 입력해 주세요. 예: /분석 전체 포트폴리오에서 미국 비중")
        return

    if not _check_ai_limit_or_reply(chat_id, ctx):
        return

    try:
        parsed = parse_analysis_with_gemini(question)
        mode = parsed.get("mode")

        if mode == "advice":
            send_message(chat_id, parsed.get("advice_text") or build_fixed_analysis_advice())
            return

        analysis_json = parsed.get("analysis_json")
        if not analysis_json:
            send_message(chat_id, build_fixed_analysis_advice())
            return

        logging.info("analysis_json=%s", json.dumps(analysis_json, ensure_ascii=False))
        retrieved = db.analyze(analysis_json)
        interpretation = summarize_analysis_json(analysis_json)
        answer = build_analysis_answer(retrieved, interpretation)
        send_message(chat_id, answer)

    except Exception as e:
        logging.exception("analysis command failed")
        send_message(chat_id, f"분석 처리 중 오류가 발생했습니다.\n{e}")


def handle_detail(chat_id: int, raw: str) -> None:
    parts = raw.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        send_message(chat_id, "Project_ID를 입력해 주세요. 예: /상세조회 BS00000669")
        return

    project_id = parts[1].strip()
    send_message(chat_id, f"{project_id} 상세 PDF를 준비 중입니다...")

    pdf_path = None
    try:
        pdf_path = util.export_project_pdf(project_id)
        util.send_document(chat_id, pdf_path, caption=f"{project_id} 상세 시트 PDF")
    except Exception as e:
        logging.exception("detail pdf export failed. project_id=%s", project_id)
        send_message(chat_id, f"상세 PDF 전송에 실패했습니다.\n에러: {e}")
    finally:
        try:
            if pdf_path and os.path.exists(pdf_path):
                os.remove(pdf_path)
        except Exception:
            pass


def process_user_message(db: "InvestmentDB", chat_id: int, text: str, ctx: Dict[str, Any]) -> None:
    raw = (text or "").strip()
    document = ctx.get("document")

    # 1) 업무 세션 중인 팀원이 파일을 올린 경우
    if document and is_active_task_session(chat_id):
        try:
            handle_task_document_reply(chat_id, document)
        except Exception as e:
            logging.exception("task document reply failed")
            send_message(chat_id, f"파일 처리 중 오류가 발생했습니다: {e}")
        return

    # 2) 업무 세션 중인 팀원이 텍스트로 답한 경우
    if raw and is_active_task_session(chat_id):
        try:
            handle_task_text_reply(chat_id, raw)
        except Exception as e:
            logging.exception("task text reply failed")
            send_message(chat_id, f"업무 답변 처리 중 오류가 발생했습니다: {e}")
        return

    # 3) 팀원 등록
    if raw.startswith("/등록"):
        handle_register_command(chat_id, raw)
        return

    # 4) 업무 지시
    if raw.startswith("/지시"):
        if str(chat_id) != str(config.OWNER_CHAT_ID):
            send_message(chat_id, "너가 뭔데 지시하고 지랄이냐")
            return
        handle_task_command(chat_id, raw)
        return

    # 5) 기존 명령어
    if raw == "/help":
        send_message(chat_id, HELP_TEXT)
        return

    if raw == "/refresh":
        try:
            db.refresh()
            send_message(chat_id, "엑셀 DB를 다시 불러왔습니다.")
        except Exception as e:
            logging.exception("refresh failed")
            send_message(chat_id, f"DB 새로고침 중 오류가 발생했습니다: {e}")
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

    if raw.startswith("/검색"):
        handle_news_search_command(chat_id, raw)
        return

    send_message(
        chat_id,
        "지원하지 않는 명령어입니다.\n"
        "/조회, /분석, /상세조회, /검색, /등록, /지시 형식으로 입력해 주세요."
    )


# =========================================================
# 메인 루프
# =========================================================
def main() -> None:
    acquire_lock(config.LOCK_FILE)

    db = InvestmentDB(config.MAIN_DB_XLSX)
    offset = load_offset()

    logging.info("Bot started.")
    print("Bot started.")

    while True:
        try:
            updates = get_updates(offset=offset, timeout=config.POLL_TIMEOUT)

            maybe_run_scheduled_news(config.OWNER_CHAT_ID)
            check_and_report_overdue_tasks()

            for upd in updates:
                offset = upd["update_id"] + 1
                save_offset(offset)

                ctx = util.extract_message_context(upd)
                chat_id = ctx.get("chat_id")
                text = ctx.get("text")

                if chat_id is None or not text:
                    continue

                try:
                    util.notify_owner_of_external_query(config.OWNER_CHAT_ID, ctx)
                except Exception:
                    logging.exception("owner notify failed")

                try:
                    process_user_message(db, chat_id, text, ctx)
                except Exception as inner_e:
                    logging.exception("message process failed")
                    try:
                        send_message(chat_id, f"처리 중 오류가 발생했습니다.\n{inner_e}")
                    except Exception:
                        logging.exception("failed to send error message")

        except requests.exceptions.ReadTimeout:
            continue
        except KeyboardInterrupt:
            logging.info("Stopped by user.")
            print("Stopped by user.")
            break
        except Exception:
            logging.exception("main loop error")
            time.sleep(config.POLL_SLEEP_ON_ERROR)


if __name__ == "__main__":
    threading.Thread(target=run_server, daemon=True).start()
    main()
