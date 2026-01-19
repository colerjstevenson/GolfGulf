from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any, Optional

import numpy as np
import pandas as pd

from pyproj import Geod
from shapely.geometry import shape


ROOT = Path(__file__).resolve().parent

CENSUS_CSV = ROOT / "data/censusShape/vancouver/vancouver_data.csv"
TRACTS_GEOJSON = ROOT / "data/censusShape/vancouver/web_assets/tracts.geojson"

METRICS_DIR = ROOT / "data/censusShape/vancouver/web_assets/metrics"
DIST_JSON = METRICS_DIR / "dist_to_golf_m.json"
AREA800_JSON = METRICS_DIR / "golf_area_within_800m_m2.json"

OUT_CSV = METRICS_DIR / "_paper_analytic_tracts.csv"
OUT_AUDIT = METRICS_DIR / "_paper_analytic_tracts_audit.json"


TARGETS = {
    "pop_2021": ["Population, 2021"],
    "income_median_hh_2020": [
        "Median total income of household in 2020 ($)",
        "Median total income of household, 2020 ($)",
        "Median total income of household in 2020",
        "Median total income of household in 2020 ($) ",
    ],
    "tenant_hh_count": [
        "Total - Tenant households in non-farm, non-reserve private dwellings - 25% sample data",
    ],
    "owner_tenant_hh_total": [
        "Total - Owner and tenant households with household total income greater than zero, in non-farm, non-reserve private dwellings by shelter-cost-to-income ratio - 25% sample data",
        "Total - Owner and tenant households with household total income greater than zero, in non-farm, non-reserve private dwellings - 25% sample data",
        "Total - Owner and tenant households",
    ],
    # If percent label exists, use it; otherwise compute from seniors count.
    "pct_65plus": ["65 years and over (%)", "  65 years and over (%)", "65 years and over (%) "],
    "count_65plus": ["65 years and over", "  65 years and over"],
}


def normalize_ctuid(x: Any) -> Optional[str]:
    if x is None:
        return None
    if isinstance(x, float) and np.isnan(x):
        return None
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


def load_metric_json(path: Path) -> Dict[str, float]:
    with open(path, "r", encoding="utf-8") as f:
        d = json.load(f)
    out: Dict[str, float] = {}
    for k, v in d.items():
        nk = normalize_ctuid(k)
        if nk is None:
            continue
        try:
            out[nk] = float(v)
        except Exception:
            out[nk] = float("nan")
    return out


def compute_tract_area_m2(tracts_geojson_path: Path) -> pd.DataFrame:
    geod = Geod(ellps="WGS84")
    with open(tracts_geojson_path, "r", encoding="utf-8") as f:
        gj = json.load(f)

    rows = []
    for feat in gj.get("features", []):
        props = feat.get("properties", {}) or {}
        ct = normalize_ctuid(props.get("CTUID"))
        if ct is None:
            continue
        geom = feat.get("geometry")
        if not geom:
            continue
        shp = shape(geom)
        try:
            area, _perim = geod.geometry_area_perimeter(shp)
            area = abs(float(area))
        except Exception:
            area = float("nan")
        rows.append({"CTUID": ct, "tract_area_m2": area})

    return pd.DataFrame(rows).drop_duplicates(subset=["CTUID"])


def best_match(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    """
    Exact match first; otherwise literal substring match (case-insensitive).
    Important: regex=False to avoid issues with parentheses like ($) and (%).
    """
    names = df["CHARACTERISTIC_NAME"].dropna().astype(str)
    name_set = set(names.tolist())

    for c in candidates:
        if c in name_set:
            return c

    low = names.str.lower()
    for c in candidates:
        pat = c.lower()
        hits = names[low.str.contains(pat, na=False, regex=False)]
        if len(hits) > 0:
            return str(hits.iloc[0])

    return None


def extract_value(df: pd.DataFrame, characteristic_name: str) -> pd.DataFrame:
    sub = df[df["CHARACTERISTIC_NAME"] == characteristic_name][
        ["CTUID", "C1_COUNT_TOTAL", "C10_RATE_TOTAL"]
    ].copy()

    v1 = pd.to_numeric(sub["C1_COUNT_TOTAL"], errors="coerce")
    v2 = pd.to_numeric(sub["C10_RATE_TOTAL"], errors="coerce")

    sub["value"] = v1
    sub.loc[sub["value"].isna(), "value"] = v2

    sub["CTUID"] = sub["CTUID"].map(normalize_ctuid)
    return sub[["CTUID", "value"]].dropna(subset=["CTUID"]).drop_duplicates(subset=["CTUID"])


def safe_stats(s: pd.Series) -> Dict[str, Any]:
    s_num = pd.to_numeric(s, errors="coerce")
    x = s_num.to_numpy()
    x = x[np.isfinite(x)]
    if x.size == 0:
        return {
            "missing_n": int(s_num.isna().sum()),
            "missing_pct": float(s_num.isna().mean() * 100.0),
            "min": None,
            "p50": None,
            "p90": None,
            "max": None,
        }
    return {
        "missing_n": int(s_num.isna().sum()),
        "missing_pct": float(s_num.isna().mean() * 100.0),
        "min": float(np.min(x)),
        "p50": float(np.percentile(x, 50)),
        "p90": float(np.percentile(x, 90)),
        "max": float(np.max(x)),
    }


def main() -> None:
    usecols = ["CENSUS_YEAR", "CTUID", "CHARACTERISTIC_NAME", "C1_COUNT_TOTAL", "C10_RATE_TOTAL"]

    if not CENSUS_CSV.exists():
        raise FileNotFoundError(f"Missing census CSV: {CENSUS_CSV}")

    census = pd.read_csv(CENSUS_CSV, usecols=usecols, low_memory=False)
    census["CTUID"] = census["CTUID"].map(normalize_ctuid)
    census["CENSUS_YEAR"] = pd.to_numeric(census["CENSUS_YEAR"], errors="coerce")
    census = census[(census["CENSUS_YEAR"] == 2021)].copy()
    census = census.dropna(subset=["CTUID", "CHARACTERISTIC_NAME"])

    resolved: Dict[str, Optional[str]] = {}
    for outvar, candidates in TARGETS.items():
        resolved[outvar] = best_match(census, candidates)

    # Require only what we truly cannot reconstruct
    must_have = ["pop_2021", "income_median_hh_2020"]
    missing = [k for k in must_have if resolved.get(k) is None]
    if missing:
        top_names = (
            census["CHARACTERISTIC_NAME"].dropna().astype(str).value_counts().head(40).index.tolist()
        )
        raise RuntimeError(
            "Could not resolve key characteristics: "
            + ", ".join(missing)
            + "\nResolved map: "
            + str(resolved)
            + "\nTop CHARACTERISTIC_NAME values (head 40): "
            + str(top_names)
        )

    print("\n=== BUILD RESOLUTION ===")
    for k, v in resolved.items():
        print(f"{k}: {v}")

    pop = extract_value(census, resolved["pop_2021"]).rename(columns={"value": "pop_2021"})
    inc = extract_value(census, resolved["income_median_hh_2020"]).rename(columns={"value": "median_hh_income_2020"})

    # renters pieces are optional (but we want them if present)
    if resolved.get("tenant_hh_count"):
        ten = extract_value(census, resolved["tenant_hh_count"]).rename(columns={"value": "tenant_hh_count"})
    else:
        ten = pd.DataFrame(columns=["CTUID", "tenant_hh_count"])

    if resolved.get("owner_tenant_hh_total"):
        tot = extract_value(census, resolved["owner_tenant_hh_total"]).rename(columns={"value": "owner_tenant_hh_total"})
    else:
        tot = pd.DataFrame(columns=["CTUID", "owner_tenant_hh_total"])

    # seniors: prefer percent label if present; else compute from count/pop
    pct65 = None
    if resolved.get("pct_65plus"):
        pct65 = extract_value(census, resolved["pct_65plus"]).rename(columns={"value": "pct_65plus"})

    cnt65 = None
    if resolved.get("count_65plus"):
        cnt65 = extract_value(census, resolved["count_65plus"]).rename(columns={"value": "count_65plus"})

    wide = pop.merge(inc, on="CTUID", how="outer") \
              .merge(ten, on="CTUID", how="outer") \
              .merge(tot, on="CTUID", how="outer")

    for c in ["pop_2021", "median_hh_income_2020", "tenant_hh_count", "owner_tenant_hh_total"]:
        if c in wide.columns:
            wide[c] = pd.to_numeric(wide[c], errors="coerce")

    # renters rate
    wide["pct_renters"] = (wide["tenant_hh_count"] / wide["owner_tenant_hh_total"]) * 100.0

    # seniors percent
    if pct65 is not None:
        wide = wide.merge(pct65, on="CTUID", how="left")
        wide["pct_65plus"] = pd.to_numeric(wide["pct_65plus"], errors="coerce")
        wide["pct_65plus_source"] = "direct_percent_label"
    elif cnt65 is not None:
        wide = wide.merge(cnt65, on="CTUID", how="left")
        wide["count_65plus"] = pd.to_numeric(wide["count_65plus"], errors="coerce")
        wide["pct_65plus"] = (wide["count_65plus"] / wide["pop_2021"]) * 100.0
        wide["pct_65plus_source"] = "computed_count_over_population"
    else:
        wide["pct_65plus"] = np.nan
        wide["pct_65plus_source"] = "missing"

    # -----------------------
    # Tract area + density
    # -----------------------
    if not TRACTS_GEOJSON.exists():
        raise FileNotFoundError(f"Missing tracts geojson: {TRACTS_GEOJSON}")

    areas = compute_tract_area_m2(TRACTS_GEOJSON)
    wide = wide.merge(areas, on="CTUID", how="left")
    wide["tract_area_km2"] = wide["tract_area_m2"] / 1_000_000.0
    wide["pop_density_per_km2"] = wide["pop_2021"] / wide["tract_area_km2"]

    # -----------------------
    # Golf exposure metrics
    # -----------------------
    if not DIST_JSON.exists():
        raise FileNotFoundError(f"Missing distance metric JSON: {DIST_JSON}")
    if not AREA800_JSON.exists():
        raise FileNotFoundError(f"Missing buffer-area metric JSON: {AREA800_JSON}")

    dist = load_metric_json(DIST_JSON)
    area800 = load_metric_json(AREA800_JSON)
    wide["dist_to_golf_m"] = wide["CTUID"].map(dist)
    wide["golf_area_within_800m_m2"] = wide["CTUID"].map(area800)

    # -----------------------
    # Audit + write
    # -----------------------
    audit: Dict[str, Any] = {
        "rows_total": int(len(wide)),
        "ctuid_unique": int(wide["CTUID"].nunique()),
        "resolved_characteristics": resolved,
        "pct_65plus_source_counts": wide["pct_65plus_source"].value_counts(dropna=False).to_dict(),
    }

    for col in [
        "pop_2021",
        "median_hh_income_2020",
        "pct_renters",
        "pct_65plus",
        "tract_area_m2",
        "pop_density_per_km2",
        "dist_to_golf_m",
        "golf_area_within_800m_m2",
    ]:
        audit[col] = safe_stats(wide[col]) if col in wide.columns else None

    wide.to_csv(OUT_CSV, index=False)
    with open(OUT_AUDIT, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("\nWrote:", OUT_CSV)
    print("Wrote:", OUT_AUDIT)

    print("\n=== QUICK MISSINGNESS CHECK ===")
    for col in ["pop_2021", "median_hh_income_2020", "pct_renters", "pct_65plus", "dist_to_golf_m", "golf_area_within_800m_m2"]:
        s = wide[col]
        print(f"{col}: missing={int(s.isna().sum())}/{len(s)} unique_nonmissing={int(s.dropna().nunique())}")

    print("\nPreview rows:\n", wide.head(3).to_string(index=False))


if __name__ == "__main__":
    main()

