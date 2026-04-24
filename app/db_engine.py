from typing import Any, Dict, List, Optional

import pandas as pd

from app import config
from app.constants import (
    ASSET_CLASS_STD_MAP,
    MANAGER_ALIAS_TO_GROUP,
    MANAGER_GROUP_KEYWORDS,
    REGION_STD_MAP,
)
from app.logger import get_logger
from app.util import (
    contains_match_norm,
    get_kst_today_year,
    normalize_text,
    safe_num,
)

logger = get_logger(__name__)


class InvestmentDB:
    def __init__(self, path: str):
        self.path = path
        self.lt: pd.DataFrame = pd.DataFrame()
        self.df = self._load()

    def refresh(self) -> None:
        self.df = self._load()

    # 신 master_portfolio.xlsx 의 Dataset 시트 한글 컬럼 → 기존 코드가 사용하는 영문 alias.
    # 기존 search/analysis 코드는 영문 컬럼 가정으로 박혀 있어서 로더에서 한 번만 갈아끼우면 변경 면적 0.
    _DATASET_COLUMN_RENAME = {
        "프로젝트명": "Asset_Name",
        "Asset_Class_EN": "Asset_Class",
        "Manager_EN": "Manager",
        "Region_EN": "Region",
        "최초인출일": "Initial_Date",
        "만기일": "Maturity_Date",
        "빈티지": "Vintage",
        "약정금액(원화)_합계": "Commitment",
        "실행(누적)_합계": "Called",
        "상환(누적)_합계": "Repaid",
        "장부가액(원화)_합계": "Outstanding",
        "평가금액(원화)_합계": "NAV",
        "수익률(원화,누적)_대표": "IRR",
        "종목ID/트렌치ID(대표)": "SubAsset_Key",
        "약정통화": "Currency",
        "투자유형": "Investment_Type",
        "세부유형": "Detail_Type",
        "자본구조1(SAP)": "Capital_Structure",
        "트렌치수": "Tranche_Count",
        "하위자산수": "Sub_Asset_Count",
    }

    @staticmethod
    def _parse_excel_date(s: pd.Series) -> pd.Series:
        """Excel serial number(45832 등) 또는 datetime 문자열을 pd.Timestamp로 통일.

        주의: pd.to_datetime(45832) 는 45832 나노초로 해석해 1970년이 나와버린다.
        따라서 numeric 우선 → Excel serial 로 처리하고, 나머지를 datetime 파싱한다.
        """
        if pd.api.types.is_datetime64_any_dtype(s):
            return s

        out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")

        # 1) numeric → Excel serial (epoch 1899-12-30, 1900 leap-year bug 보정)
        numeric = pd.to_numeric(s, errors="coerce")
        serial_mask = numeric.notna()
        if serial_mask.any():
            serial_dt = pd.to_datetime(
                numeric.where(serial_mask), unit="D", origin="1899-12-30", errors="coerce"
            )
            # Excel max 2958465 = 9999-12-31 (만기 미정 placeholder) → NaT
            serial_dt = serial_dt.where(serial_dt.dt.year < 9000, pd.NaT)
            out = out.where(~serial_mask, serial_dt)

        # 2) numeric 으로 잡히지 않은 값은 일반 datetime 문자열 시도
        str_mask = (~serial_mask) & s.notna()
        if str_mask.any():
            out = out.where(~str_mask, pd.to_datetime(s.where(str_mask), errors="coerce"))

        return out

    def _load(self) -> pd.DataFrame:
        df = pd.read_excel(self.path, sheet_name=config.MAIN_DB_SHEET)
        df = df.rename(columns=self._DATASET_COLUMN_RENAME)

        # Dataset 시트에는 빈 placeholder 행 (Project_ID 비어있음)이 섞여 있다 —
        # 필터링된 프로젝트 및 미사용 예비 행들이 그 원인. 실제 BS* 행만 남긴다.
        if "Project_ID" in df.columns:
            pid_col = df["Project_ID"].astype(str).str.strip()
            df = df[pid_col.str.match(r"^BS\d", na=False)].copy()

        required = [
            "Project_ID", "Asset_Name", "Asset_Class", "Manager", "Region",
            "Strategy", "Sector", "Initial_Date", "Vintage", "Maturity_Date",
            "Commitment", "Called", "Outstanding", "NAV", "IRR",
            "Currency", "SubAsset_Key", "Tranche_Count", "Sub_Asset_Count",
        ]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"필수 컬럼 누락: {missing}")

        text_cols = [
            "Project_ID", "Asset_Class", "Asset_Name", "Manager", "Region",
            "Strategy", "Sector", "Currency", "Investment_Type", "Detail_Type",
            "Capital_Structure",
        ]
        for c in text_cols:
            if c in df.columns:
                df[c] = df[c].fillna("").astype(str).str.strip()

        # 약정통화 = "미인출" 또는 빈값 → Unknown (인출 전이라 통화 미확정)
        df["Currency"] = df["Currency"].replace({"": "Unknown", "미인출": "Unknown"})

        # 빈티지: "~2016", "미인출" 같은 문자열은 NaN 처리
        df["Vintage"] = pd.to_numeric(df["Vintage"], errors="coerce")

        # 날짜: Excel serial 변환
        df["Initial_Date"] = self._parse_excel_date(df["Initial_Date"])
        df["Maturity_Date"] = self._parse_excel_date(df["Maturity_Date"])
        df["Maturity_Year"] = df["Maturity_Date"].dt.year

        # 금액 컬럼: 원화 원단위 → 억 단위 (기존 format_amount_uk 가 "{:,.0f}억" 가정)
        for c in ["Commitment", "Called", "Repaid", "Outstanding", "NAV"]:
            df[c] = pd.to_numeric(df[c], errors="coerce") / 1e8

        # IRR: % 표기 → 소수 (기존 format_pct 와 IRR 안전망이 소수 가정)
        df["IRR"] = pd.to_numeric(df["IRR"], errors="coerce") / 100.0

        for c in ["Tranche_Count", "Sub_Asset_Count"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

        # SubAsset_Key: LookThrough 조인 키 (nullable int)
        df["SubAsset_Key"] = pd.to_numeric(df["SubAsset_Key"], errors="coerce").astype("Int64")

        df["Asset_Class_Std"] = df["Asset_Class"].apply(self._std_asset_class)
        df["Region_Std"] = df["Region"].apply(self._std_region)
        df["Manager_Norm"] = df["Manager"].apply(normalize_text)
        df["Asset_Name_Norm"] = df["Asset_Name"].apply(normalize_text)
        df["Strategy_Norm"] = df["Strategy"].apply(normalize_text)
        df["Sector_Norm"] = df["Sector"].apply(normalize_text)
        df["Project_ID_Norm"] = df["Project_ID"].apply(normalize_text)

        self._load_lookthrough()
        logger.info(
            "DB loaded | rows=%d | file=%s | lt_rows=%d",
            len(df), self.path, len(self.lt) if self.lt is not None else 0,
        )
        return df

    def _load_lookthrough(self) -> None:
        try:
            lt = pd.read_excel(self.path, sheet_name=config.LT_SHEET)
        except Exception:
            logger.exception("LookThrough load failed | file=%s", self.path)
            self.lt = pd.DataFrame()
            return

        lt = lt.rename(columns={
            "펀드 종목ID": "Fund_SubAsset_Key",
            "펀드 종목명": "Fund_Name",
            "수익증권KEY": "Vehicle_Key",
            "하위자산유형": "Sub_Type",
            "편입자산 ID": "Holding_ID",
            "편입자산 종목명": "Holding_Name",
            "상품구분": "Product_Type",
            "거래상대방/발행인": "Counterparty",
            "포지션통화": "Position_Currency",
            "장부금액(원화)": "Book_Value",
            "금리(%)": "Coupon_Rate",
            "만기일": "Holding_Maturity",
            "매입일": "Purchase_Date",
        })
        lt["Fund_SubAsset_Key"] = pd.to_numeric(lt["Fund_SubAsset_Key"], errors="coerce").astype("Int64")
        lt["Book_Value"] = pd.to_numeric(lt["Book_Value"], errors="coerce") / 1e8  # 억 단위
        lt["Coupon_Rate"] = pd.to_numeric(lt["Coupon_Rate"], errors="coerce")
        lt["Holding_Maturity"] = self._parse_excel_date(lt["Holding_Maturity"])
        lt["Purchase_Date"] = self._parse_excel_date(lt["Purchase_Date"])
        for c in ["Sub_Type", "Product_Type", "Counterparty", "Position_Currency", "Fund_Name", "Holding_Name"]:
            if c in lt.columns:
                lt[c] = lt[c].fillna("").astype(str).str.strip()
        self.lt = lt

    def lookthrough_for(self, project_id: str) -> pd.DataFrame:
        """단일 펀드의 LookThrough 하위자산 subset 반환. 없으면 빈 DataFrame."""
        if not project_id or self.lt is None or self.lt.empty:
            return pd.DataFrame()
        proj = self.df[self.df["Project_ID"] == project_id]
        if proj.empty:
            return pd.DataFrame()
        key = proj.iloc[0].get("SubAsset_Key")
        if pd.isna(key):
            return pd.DataFrame()
        return self.lt[self.lt["Fund_SubAsset_Key"] == key].copy()

    @staticmethod
    def _std_asset_class(x: str) -> str:
        nx = normalize_text(x)
        return ASSET_CLASS_STD_MAP.get(nx, x)

    @staticmethod
    def _std_region(x: str) -> str:
        nx = normalize_text(x)
        return REGION_STD_MAP.get(nx, x)

    @staticmethod
    def _expand_manager_keywords(managers: List[str]) -> List[str]:
        expanded: List[str] = []

        for m in managers:
            raw = str(m).strip()
            if not raw:
                continue

            norm = normalize_text(raw)
            group_name = MANAGER_ALIAS_TO_GROUP.get(norm)

            if group_name:
                expanded.extend(MANAGER_GROUP_KEYWORDS.get(group_name, []))
                continue

            matched_group = None
            for alias_norm, gname in MANAGER_ALIAS_TO_GROUP.items():
                if norm == alias_norm or norm in alias_norm or alias_norm in norm:
                    matched_group = gname
                    break

            if matched_group:
                expanded.extend(MANAGER_GROUP_KEYWORDS.get(matched_group, []))
            else:
                expanded.append(raw)

        seen = set()
        out: List[str] = []
        for x in expanded:
            nx = normalize_text(x)
            if nx and nx not in seen:
                seen.add(nx)
                out.append(x)
        return out

    def _exclude_matured(self, df: pd.DataFrame) -> pd.DataFrame:
        current_year = get_kst_today_year()
        return df[
            df["Maturity_Year"].isna() | (df["Maturity_Year"] >= current_year)
        ].copy()

    def _apply_filters(self, base_df: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
        df = base_df.copy()

        asset_classes = filters.get("asset_class") or []
        if asset_classes:
            df = df[df["Asset_Class_Std"].isin(asset_classes)]

        regions = filters.get("region") or []
        if regions:
            df = df[df["Region_Std"].isin(regions)]

        strategies = filters.get("strategy") or []
        if strategies:
            mask = pd.Series([True] * len(df), index=df.index)
            for s in strategies:
                mask &= contains_match_norm(df["Strategy_Norm"], s)
            df = df[mask]

        sectors = filters.get("sector") or []
        if sectors:
            mask = pd.Series([False] * len(df), index=df.index)
            for s in sectors:
                mask |= contains_match_norm(df["Sector_Norm"], s)
            df = df[mask]

        managers = filters.get("manager") or []
        if managers:
            keywords = self._expand_manager_keywords(managers)
            not_blank = df["Manager"].fillna("").astype(str).str.strip() != ""
            mask = pd.Series([False] * len(df), index=df.index)
            for kw in keywords:
                mask |= contains_match_norm(df["Manager_Norm"], kw)
            df = df[not_blank & mask]

        project_ids = filters.get("project_id") or []
        if project_ids:
            norm_ids = [normalize_text(x) for x in project_ids if str(x).strip()]
            df = df[df["Project_ID_Norm"].isin(norm_ids)]

        fund_name_keywords = filters.get("fund_name_keywords") or []
        if fund_name_keywords:
            mask = pd.Series([False] * len(df), index=df.index)
            for kw in fund_name_keywords:
                mask |= contains_match_norm(df["Asset_Name_Norm"], kw)
            df = df[mask]

        asset_name_keywords = filters.get("asset_name_keywords") or []
        if asset_name_keywords:
            mask = pd.Series([False] * len(df), index=df.index)
            for kw in asset_name_keywords:
                mask |= contains_match_norm(df["Asset_Name_Norm"], kw)
            df = df[mask]

        for (min_key, max_key, col) in [
            ("vintage_from", "vintage_to", "Vintage"),
            ("maturity_year_from", "maturity_year_to", "Maturity_Year"),
            ("irr_min", "irr_max", "IRR"),
            ("commit_min", "commit_max", "Commitment"),
            ("called_min", "called_max", "Called"),
            ("outstanding_min", "outstanding_max", "Outstanding"),
            ("nav_min", "nav_max", "NAV"),
        ]:
            vmin = filters.get(min_key)
            vmax = filters.get(max_key)
            if vmin is not None:
                try:
                    df = df[df[col] >= float(vmin)]
                except (TypeError, ValueError):
                    pass
            if vmax is not None:
                try:
                    df = df[df[col] <= float(vmax)]
                except (TypeError, ValueError):
                    pass

        return self._exclude_matured(df)

    @staticmethod
    def _project_level_df(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=[
                "Project_ID", "Asset_Name", "Manager", "Asset_Class_Std", "Region_Std",
                "Strategy", "Sector", "Vintage", "Maturity_Year",
                "Commitment", "Called", "Outstanding", "NAV", "IRR",
                "Sub_Asset_Count", "Currency",
            ])
        return (
            df.groupby("Project_ID", as_index=False)
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
                "Sub_Asset_Count": "first",
                "Currency": "first",
            })
            .copy()
        )

    def search(self, query_json: Dict[str, Any]) -> Dict[str, Any]:
        filters = query_json.get("filters", {}) or {}
        filtered_df = self._apply_filters(self.df, filters)
        project_df = self._project_level_df(filtered_df)

        sort_cfg = query_json.get("sort", {}) or {}
        sort_col_map = {
            "irr": "IRR",
            "commitment": "Commitment",
            "called": "Called",
            "outstanding": "Outstanding",
            "nav": "NAV",
            "maturity_year": "Maturity_Year",
        }
        sort_col = sort_col_map.get(sort_cfg.get("by"))

        if sort_col and sort_col in project_df.columns:
            ascending = sort_cfg.get("order") == "asc"
            project_df = project_df.sort_values(sort_col, ascending=ascending, na_position="last")
        else:
            project_df = project_df.sort_values(
                ["Commitment", "Project_ID"], ascending=[False, True], na_position="last"
            )

        limit = int(query_json.get("output", {}).get("limit", config.DEFAULT_LIMIT) or config.DEFAULT_LIMIT)
        limit = max(1, min(limit, config.MAX_LIMIT))
        rows_df = project_df.head(limit).copy()

        rows: List[Dict[str, Any]] = []
        for _, row in rows_df.iterrows():
            rows.append({
                "Project_ID": row["Project_ID"],
                "Asset_Name": row["Asset_Name"],
                "Manager": row["Manager"] or None,
                "Asset_Class": row["Asset_Class_Std"],
                "Region": row["Region_Std"],
                "Strategy": row["Strategy"] or None,
                "Sector": row["Sector"] or None,
                "Vintage": safe_num(row["Vintage"]),
                "Maturity_Year": safe_num(row["Maturity_Year"]),
                "Commitment": safe_num(row["Commitment"]),
                "Called": safe_num(row["Called"]),
                "Outstanding": safe_num(row["Outstanding"]),
                "NAV": safe_num(row["NAV"]),
                "IRR": safe_num(row["IRR"]),
                "Sub_Asset_Count": int(row["Sub_Asset_Count"]) if "Sub_Asset_Count" in rows_df.columns and pd.notna(row.get("Sub_Asset_Count")) else 0,
                "Currency": (row.get("Currency") or None) if "Currency" in rows_df.columns else None,
            })

        weighted_irr: Optional[float] = None
        valid_irr = project_df.dropna(subset=["IRR", "Commitment"])
        if not valid_irr.empty and valid_irr["Commitment"].sum() > 0:
            weighted_irr = (valid_irr["IRR"] * valid_irr["Commitment"]).sum() / valid_irr["Commitment"].sum()

        summary = {
            "count_raw_rows": int(len(filtered_df)),
            "count_projects_total": int(project_df["Project_ID"].nunique()) if not project_df.empty else 0,
            "sum_commitment": safe_num(project_df["Commitment"].sum()) if not project_df.empty else 0.0,
            "sum_called": safe_num(project_df["Called"].sum()) if not project_df.empty else 0.0,
            "sum_outstanding": safe_num(project_df["Outstanding"].sum()) if not project_df.empty else 0.0,
            "sum_nav": safe_num(project_df["NAV"].sum()) if not project_df.empty else 0.0,
            "avg_irr": safe_num(weighted_irr),
        }

        return {
            "result_type": "search",
            "query_json": query_json,
            "summary": summary,
            "rows": rows,
            "detail_project_ids": rows_df["Project_ID"].tolist() if not rows_df.empty else [],
        }

    def analyze(self, analysis_json: Dict[str, Any]) -> Dict[str, Any]:
        atype = analysis_json.get("analysis_type")
        if atype == "share":
            return self._analyze_share(analysis_json)
        if atype == "grouped_metric":
            return self._analyze_grouped_metric(analysis_json)
        raise ValueError(f"지원하지 않는 analysis_type: {atype}")

    @staticmethod
    def _metric_value(project_df: pd.DataFrame, metric: str) -> Optional[float]:
        if project_df.empty:
            return 0.0
        if metric == "count":
            return float(project_df["Project_ID"].nunique())
        if metric == "commitment":
            return safe_num(project_df["Commitment"].sum()) or 0.0
        if metric == "called":
            return safe_num(project_df["Called"].sum()) or 0.0
        if metric == "outstanding":
            return safe_num(project_df["Outstanding"].sum()) or 0.0
        if metric == "nav":
            return safe_num(project_df["NAV"].sum()) or 0.0

        valid = project_df.dropna(subset=["IRR"]).copy()
        if valid.empty:
            return None
        if metric == "irr_avg":
            return safe_num(valid["IRR"].mean())

        weight_col_map = {
            "irr_weighted_commitment": "Commitment",
            "irr_weighted_called": "Called",
            "irr_weighted_outstanding": "Outstanding",
            "irr_weighted_nav": "NAV",
        }
        weight_col = weight_col_map.get(metric)
        if weight_col:
            valid = valid.dropna(subset=[weight_col])
            denom = valid[weight_col].sum()
            if denom > 0:
                return safe_num((valid["IRR"] * valid[weight_col]).sum() / denom)
            return None

        raise ValueError(f"지원하지 않는 metric: {metric}")

    @staticmethod
    def _groupby_col(groupby: str) -> str:
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
        if base_value and base_value != 0 and target_value is not None:
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
        rows: List[Dict[str, Any]] = []

        for keys, grp in base_project_df.groupby(group_cols, dropna=False):
            if not isinstance(keys, tuple):
                keys = (keys,)
            labels = []
            for val in keys:
                if pd.isna(val) or str(val).strip() == "":
                    labels.append("N/A")
                else:
                    labels.append(str(val))
            row = {
                "group": labels,
                "project_count": int(grp["Project_ID"].nunique()),
            }
            for m in metrics:
                row[m] = self._metric_value(grp, m)
            rows.append(row)

        reverse = sort_order != "asc"
        rows = [r for r in rows if r.get(sort_by) is not None]
        rows.sort(key=lambda x: x.get(sort_by, -1e18), reverse=reverse)
        rows = rows[:top_n]

        return {
            "result_type": "analysis",
            "analysis_type": "grouped_metric",
            "groupby": groupby,
            "metrics": metrics,
            "sort_by": sort_by,
            "rows": rows,
            "base_project_count": int(base_project_df["Project_ID"].nunique()),
        }

    # =========================================================
    # Compact context for Gemini (used by E2 news→portfolio, F3 task ctx)
    # =========================================================
    def portfolio_impact_summary(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        result = self.search({"filters": filters, "output": {"limit": 1}})
        s = result["summary"]
        return {
            "count": s["count_projects_total"],
            "sum_commitment": s["sum_commitment"],
            "sum_outstanding": s["sum_outstanding"],
            "sum_nav": s["sum_nav"],
            "avg_irr": s["avg_irr"],
        }

    def project_context(self, project_id: str) -> Optional[Dict[str, Any]]:
        if not project_id:
            return None
        filters = {"project_id": [project_id]}
        res = self.search({"filters": filters, "output": {"limit": 1}})
        rows = res.get("rows") or []
        if not rows:
            return None
        return rows[0]

    # =========================================================
    # Project reference resolver (BS\d+ 또는 자유 텍스트 → Project_ID 후보)
    # =========================================================
    def resolve_project_ref(self, ref: str, limit: int = 5) -> List[Dict[str, Any]]:
        """자유 텍스트를 Project_ID 후보 리스트로 변환.
        - 'BS00000XXX' 정확 매칭 우선.
        - 아니면 Asset_Name / Manager 를 normalize 후 contains 매칭.
        결과: [{project_id, asset_name, manager, asset_class, sub_asset_count}, ...] (최대 limit)."""
        if not ref or self.df is None or self.df.empty:
            return []
        s = str(ref).strip()
        if not s:
            return []

        import re as _re
        m = _re.search(r"BS\d{6,10}", s, _re.IGNORECASE)
        if m:
            pid = m.group(0).upper()
            exact = self.df[self.df["Project_ID"] == pid]
            if not exact.empty:
                r = exact.iloc[0]
                return [{
                    "project_id": str(r["Project_ID"]),
                    "asset_name": str(r.get("Asset_Name") or ""),
                    "manager": str(r.get("Manager") or ""),
                    "asset_class": str(r.get("Asset_Class") or ""),
                    "sub_asset_count": int(r.get("Sub_Asset_Count") or 0),
                }]

        # 공백 포함 문자열 → 토큰 단위로 전부 포함하는 행 찾기 (AND 매칭)
        tokens = [t for t in _re.split(r"\s+", s) if t]
        if not tokens:
            return []
        mask = pd.Series([True] * len(self.df), index=self.df.index)
        for t in tokens:
            kw = normalize_text(t)
            if not kw:
                continue
            name_hit = self.df["Asset_Name_Norm"].fillna("").astype(str).str.contains(kw, regex=False, na=False)
            mgr_hit = self.df["Manager_Norm"].fillna("").astype(str).str.contains(kw, regex=False, na=False)
            mask &= (name_hit | mgr_hit)
        cands = self.df[mask].head(limit)
        out = []
        for _, r in cands.iterrows():
            out.append({
                "project_id": str(r["Project_ID"]),
                "asset_name": str(r.get("Asset_Name") or ""),
                "manager": str(r.get("Manager") or ""),
                "asset_class": str(r.get("Asset_Class") or ""),
                "sub_asset_count": int(r.get("Sub_Asset_Count") or 0),
            })
        return out

    # =========================================================
    # LookThrough — Phase 1: 단일 펀드 드릴다운
    # =========================================================
    def lookthrough_summary(self, project_id: str) -> Optional[Dict[str, Any]]:
        """단일 펀드의 LT 요약 (자산유형/통화 mix, 가중평균금리, top holdings, 만기 사다리).
        룩쓰루 데이터가 없는 펀드(`Sub_Asset_Count == 0`)는 None 반환."""
        if not project_id:
            return None
        proj = self.df[self.df["Project_ID"] == project_id]
        if proj.empty:
            return None
        p = proj.iloc[0]

        lt = self.lookthrough_for(project_id)

        base = {
            "project_id": project_id,
            "asset_name": str(p.get("Asset_Name") or ""),
            "asset_class": str(p.get("Asset_Class") or ""),
            "manager": str(p.get("Manager") or ""),
            "region": str(p.get("Region") or ""),
            "currency": str(p.get("Currency") or ""),
            "fund_commitment": safe_num(p.get("Commitment")),
            "fund_outstanding": safe_num(p.get("Outstanding")),
            "fund_irr": safe_num(p.get("IRR")),
            "tranche_count": int(p.get("Tranche_Count") or 0),
            "sub_asset_count": int(p.get("Sub_Asset_Count") or 0),
            "lt_count": int(len(lt)),
            "lt_book_total": safe_num(lt["Book_Value"].sum()) if not lt.empty else 0.0,
            "subtype_share": [],
            "currency_share": [],
            "weighted_coupon": None,
            "top_holdings": [],
            "maturity_buckets": {"<=1y": 0.0, "1-3y": 0.0, "3y+": 0.0, "no_maturity": 0.0},
        }

        if lt.empty:
            return base

        # 자산유형 mix (장부 비중)
        st = lt.groupby("Sub_Type")["Book_Value"].agg(["sum", "count"]).sort_values("sum", ascending=False)
        total_book = float(lt["Book_Value"].sum() or 0.0)
        for st_name, row in st.iterrows():
            book = float(row["sum"] or 0.0)
            base["subtype_share"].append({
                "sub_type": str(st_name) or "N/A",
                "count": int(row["count"]),
                "book": book,
                "share": (book / total_book) if total_book else None,
            })

        # 통화 mix
        cc = lt.groupby("Position_Currency")["Book_Value"].sum().sort_values(ascending=False)
        for ccy, book in cc.items():
            book_v = float(book or 0.0)
            base["currency_share"].append({
                "currency": str(ccy) or "Unknown",
                "book": book_v,
                "share": (book_v / total_book) if total_book else None,
            })

        # 가중평균 보유금리 (대출/채권만)
        rate_df = lt[lt["Sub_Type"].isin(["대출", "채권"]) & lt["Coupon_Rate"].notna() & (lt["Book_Value"] > 0)]
        if not rate_df.empty:
            denom = float(rate_df["Book_Value"].sum() or 0.0)
            if denom > 0:
                base["weighted_coupon"] = float((rate_df["Coupon_Rate"] * rate_df["Book_Value"]).sum() / denom)

        # TOP 10 holdings
        top = lt.nlargest(10, "Book_Value")
        for _, row in top.iterrows():
            base["top_holdings"].append({
                "name": str(row.get("Holding_Name") or "") or str(row.get("Counterparty") or ""),
                "counterparty": str(row.get("Counterparty") or ""),
                "sub_type": str(row.get("Sub_Type") or ""),
                "currency": str(row.get("Position_Currency") or ""),
                "book": safe_num(row.get("Book_Value")),
                "coupon": safe_num(row.get("Coupon_Rate")),
            })

        # 만기 사다리
        from datetime import timedelta
        from app.util import get_kst_now
        today = get_kst_now()
        m = lt["Holding_Maturity"]
        days = (m - pd.Timestamp(today.date())).dt.days

        b1 = lt[days.between(0, 365)]["Book_Value"].sum()
        b13 = lt[days.between(366, 365 * 3)]["Book_Value"].sum()
        b3p = lt[days > 365 * 3]["Book_Value"].sum()
        bna = lt[m.isna() | (days < 0)]["Book_Value"].sum()

        base["maturity_buckets"] = {
            "<=1y": safe_num(b1) or 0.0,
            "1-3y": safe_num(b13) or 0.0,
            "3y+": safe_num(b3p) or 0.0,
            "no_maturity": safe_num(bna) or 0.0,
        }
        return base

    # =========================================================
    # LookThrough — Phase 2: 익스포저 역방향 조회
    # =========================================================
    def _exposure_match_mask(self, mode: str, query: str) -> pd.Series:
        if self.lt is None or self.lt.empty:
            return pd.Series(dtype=bool)
        nq = normalize_text(query)
        if not nq:
            return pd.Series([False] * len(self.lt), index=self.lt.index)

        cp_norm = self.lt["Counterparty"].fillna("").astype(str).map(normalize_text)
        if mode == "counterparty":
            mask = cp_norm.str.contains(nq, regex=False, na=False)
        else:  # holding (broader: counterparty + holding name)
            hn_norm = self.lt["Holding_Name"].fillna("").astype(str).map(normalize_text)
            mask = cp_norm.str.contains(nq, regex=False, na=False) | hn_norm.str.contains(nq, regex=False, na=False)
        return mask

    def exposure_search(self, mode: str, query: str, fund_top_n: int = 20) -> Dict[str, Any]:
        """LT에서 발행인/종목 단위 노출 역조회.
        mode: 'counterparty' (Counterparty만) | 'holding' (Counterparty + Holding_Name)"""
        result = {
            "mode": mode,
            "query": query,
            "match_lt_rows": 0,
            "match_book": 0.0,
            "fund_count": 0,
            "org_lt_total": safe_num(self.lt["Book_Value"].sum()) if (self.lt is not None and not self.lt.empty) else 0.0,
            "share": None,
            "by_fund": [],
            "by_currency": [],
        }
        if self.lt is None or self.lt.empty or not query.strip():
            return result

        mask = self._exposure_match_mask(mode, query)
        sub = self.lt[mask].copy()
        if sub.empty:
            return result

        result["match_lt_rows"] = int(len(sub))
        result["match_book"] = safe_num(sub["Book_Value"].sum()) or 0.0
        if result["org_lt_total"]:
            result["share"] = result["match_book"] / result["org_lt_total"]

        # 펀드별 집계 — Fund_SubAsset_Key + Fund_Name (Dataset 미매칭 케이스 대비)
        by_key = (
            sub.groupby(["Fund_SubAsset_Key", "Fund_Name"], dropna=False)
            .agg(lt_book=("Book_Value", "sum"), lt_count=("Book_Value", "size"))
            .reset_index()
            .sort_values("lt_book", ascending=False)
        )

        # Dataset에서 펀드 메타 join (SubAsset_Key 일치)
        meta = self.df[["Project_ID", "Asset_Name", "Manager", "Asset_Class", "SubAsset_Key", "Currency"]].copy()
        joined = by_key.merge(
            meta, left_on="Fund_SubAsset_Key", right_on="SubAsset_Key", how="left"
        )
        result["fund_count"] = int(joined["Project_ID"].notna().sum())
        result["unmatched_fund_count"] = int(joined["Project_ID"].isna().sum())

        for _, row in joined.head(fund_top_n).iterrows():
            matched = pd.notna(row.get("Project_ID"))
            result["by_fund"].append({
                "project_id": str(row["Project_ID"]) if matched else "",
                "asset_name": (
                    str(row["Asset_Name"]) if matched
                    else f"{row.get('Fund_Name') or 'Unknown'} (Dataset 미매칭)"
                ),
                "manager": str(row["Manager"]) if matched else "",
                "asset_class": str(row["Asset_Class"]) if matched else "",
                "fund_currency": str(row["Currency"]) if matched else "",
                "lt_book": safe_num(row["lt_book"]),
                "lt_count": int(row["lt_count"]),
                "matched": matched,
            })

        # 통화 분포 (매칭 LT만)
        by_ccy = sub.groupby("Position_Currency")["Book_Value"].sum().sort_values(ascending=False)
        for ccy, book in by_ccy.items():
            result["by_currency"].append({
                "currency": str(ccy) or "Unknown",
                "book": safe_num(book) or 0.0,
            })
        return result

    def top_managers_by_outstanding(self, limit: int = 10, overseas_only: bool = False) -> List[str]:
        import re as _re
        from app.constants import OVERSEAS_REGIONS
        df = self.df
        if df is None or df.empty or "Manager" not in df.columns:
            return []
        if overseas_only and "Region_Std" in df.columns:
            df = df[df["Region_Std"].isin(OVERSEAS_REGIONS)]
        normalized = (
            df["Manager"]
            .fillna("")
            .astype(str)
            .map(lambda s: _re.sub(r"\s+", " ", s).strip())
        )
        grouped = (
            df.assign(_Manager=normalized)
            .loc[lambda d: d["_Manager"].ne("") & d["_Manager"].ne("-")]
            .groupby("_Manager")["Outstanding"]
            .sum()
            .sort_values(ascending=False)
        )
        return [str(m) for m in grouped.head(limit).index.tolist()]
