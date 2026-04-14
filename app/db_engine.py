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
        self.df = self._load()

    def refresh(self) -> None:
        self.df = self._load()

    def _load(self) -> pd.DataFrame:
        df = pd.read_excel(self.path)

        expected = [
            "Project_ID", "Asset_Class", "Asset_Name", "Manager", "Region",
            "Strategy", "Sector", "Initial_Date", "Vintage",
            "Investment_Period", "Maturity_Date", "Maturity_Year",
            "Commitment", "Called", "Outstanding", "NAV", "IRR",
        ]
        missing = [c for c in expected if c not in df.columns]
        if missing:
            raise ValueError(f"필수 컬럼 누락: {missing}")

        text_cols = ["Project_ID", "Asset_Class", "Asset_Name", "Manager", "Region", "Strategy", "Sector"]
        for c in text_cols:
            df[c] = df[c].fillna("").astype(str).str.strip()

        for c in ["Initial_Date", "Investment_Period", "Maturity_Date"]:
            df[c] = pd.to_datetime(df[c], errors="coerce")

        for c in ["Vintage", "Maturity_Year", "Commitment", "Called", "Outstanding", "NAV", "IRR"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df["Asset_Class_Std"] = df["Asset_Class"].apply(self._std_asset_class)
        df["Region_Std"] = df["Region"].apply(self._std_region)
        df["Manager_Norm"] = df["Manager"].apply(normalize_text)
        df["Asset_Name_Norm"] = df["Asset_Name"].apply(normalize_text)
        df["Strategy_Norm"] = df["Strategy"].apply(normalize_text)
        df["Sector_Norm"] = df["Sector"].apply(normalize_text)
        df["Project_ID_Norm"] = df["Project_ID"].apply(normalize_text)

        logger.info("DB loaded | rows=%d | file=%s", len(df), self.path)
        return df

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

    def top_managers_by_outstanding(self, limit: int = 10) -> List[str]:
        import re as _re
        df = self.df
        if df is None or df.empty or "Manager" not in df.columns:
            return []
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
