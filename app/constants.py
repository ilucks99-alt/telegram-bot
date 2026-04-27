from typing import Dict, List, Tuple

from app.util import normalize_text

# =========================================================
# Allowed domain values
# =========================================================
ASSET_CLASS_ALLOWED = {"Real_Estate", "PE", "VC", "PD", "Infrastructure"}
REGION_ALLOWED = {"US", "Europe", "Asia", "Global", "KOR", "MENA", "Canada"}
OVERSEAS_REGIONS = ["US", "Europe", "Asia", "Global", "MENA", "Canada"]
CURRENCY_ALLOWED = {"KRW", "USD", "EUR", "GBP", "JPY", "Unknown"}

SORT_BY_ALLOWED = {
    "irr", "commitment", "called", "outstanding", "nav", "maturity_year",
    "repaid", "dpi", "tvpi", "drawdown", "unfunded",
}
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


# =========================================================
# Manager groups + aliases (A2: TPG comma bug fixed)
# =========================================================
MANAGER_GROUP_MEMBERS: Dict[str, List[str]] = {
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

MANAGER_GROUP_ALIASES: Dict[str, List[str]] = {
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
        "angelo gordon",
        "tpg angelo gordon",
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


def _build_manager_group_maps() -> Tuple[Dict[str, str], Dict[str, List[str]]]:
    alias_to_group: Dict[str, str] = {}
    group_to_keywords: Dict[str, List[str]] = {}

    for group_name, members in MANAGER_GROUP_MEMBERS.items():
        merged = [group_name] + members + MANAGER_GROUP_ALIASES.get(group_name, [])

        dedup_norm = set()
        keywords: List[str] = []

        for item in merged:
            s = str(item).strip()
            if not s:
                continue
            ns = normalize_text(s)
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
# Asset class / region standardization maps
# =========================================================
ASSET_CLASS_STD_MAP = {
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

REGION_STD_MAP = {
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
