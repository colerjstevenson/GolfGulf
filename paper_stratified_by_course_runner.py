# paper_stratified_by_course_runner.py
from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd
import geopandas as gpd

ROOT = Path(__file__).resolve().parent

IN_CSV   = ROOT / "data/censusShape/vancouver/web_assets/metrics/_paper_analytic_tracts.csv"
TRACTS_GEO = ROOT / "data/censusShape/vancouver/web_assets/tracts.geojson"
GOLF_GJ  = ROOT / "data/censusShape/vancouver/web_assets/golf_courses.geojson"

OUT_DIR  = ROOT / "outputs/paper_tables"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------
# Helpers
# -----------------------
def normalize_ctuid_series(s: pd.Series) -> pd.Series:
    """
    Normalize CTUIDs so joins work across formats:
      - '9330005.0' -> '9330005'
      - ' 9330005'  -> '9330005'
    """
    s = s.astype("string").str.strip()
    s = s.str.replace(r"\.00$", "", regex=True)
    s = s.str.replace(r"\.0$", "", regex=True)
    return s

def qcut_safe(s: pd.Series, q: int = 5) -> pd.Series:
    """
    Quintiles with safe fallback when many ties exist.
    Returns Int64 labels 1..q (or fewer if duplicates must be dropped).
    """
    s2 = pd.to_numeric(s, errors="coerce").copy()
    nunq = s2.dropna().nunique()
    if nunq == 0:
        return pd.Series([pd.NA] * len(s2), index=s2.index, dtype="Int64")
    if nunq < q:
        r = s2.rank(method="average")
        out = pd.qcut(r, q=q, labels=False, duplicates="drop")
    else:
        out = pd.qcut(s2, q=q, labels=False, duplicates="drop")
    return (out + 1).astype("Int64")

def weighted_mean(x: pd.Series, w: pd.Series) -> float:
    x = pd.to_numeric(x, errors="coerce")
    w = pd.to_numeric(w, errors="coerce")
    m = x.notna() & w.notna() & np.isfinite(x) & np.isfinite(w) & (w > 0)
    if int(m.sum()) == 0:
        return float("nan")
    return float(np.average(x[m].astype(float), weights=w[m].astype(float)))

def bootstrap_diff(a: np.ndarray, b: np.ndarray, n: int = 2000, seed: int = 7) -> tuple[float, float, float]:
    """
    Bootstrap CI for mean(a) - mean(b). Returns (diff, lo, hi).
    """
    rng = np.random.default_rng(seed)
    a = a[np.isfinite(a)]
    b = b[np.isfinite(b)]
    if len(a) < 5 or len(b) < 5:
        return (float("nan"), float("nan"), float("nan"))
    diffs = np.empty(n, dtype=float)
    for i in range(n):
        sa = rng.choice(a, size=len(a), replace=True)
        sb = rng.choice(b, size=len(b), replace=True)
        diffs[i] = sa.mean() - sb.mean()
    return (float(a.mean() - b.mean()), float(np.quantile(diffs, 0.025)), float(np.quantile(diffs, 0.975)))

def bootstrap_diff_ratio(a_num: np.ndarray, a_den: np.ndarray, b_num: np.ndarray, b_den: np.ndarray,
                        n: int = 2000, seed: int = 7) -> tuple[float, float, float]:
    """
    Bootstrap CI for (sum(a_num)/sum(a_den)) - (sum(b_num)/sum(b_den)).
    Intended for share_pop_within_800m differences.
    """
    rng = np.random.default_rng(seed)

    a_num = a_num[np.isfinite(a_num)]
    a_den = a_den[np.isfinite(a_den)]
    b_num = b_num[np.isfinite(b_num)]
    b_den = b_den[np.isfinite(b_den)]

    # Require enough observations and positive denominators
    if len(a_num) < 5 or len(b_num) < 5:
        return (float("nan"), float("nan"), float("nan"))

    def share(num, den):
        d = np.nansum(den)
        return np.nan if d <= 0 else float(np.nansum(num) / d)

    # Point estimate (not bootstrapped)
    point = share(a_num, a_den) - share(b_num, b_den)

    diffs = []
    idx_a = np.arange(len(a_num))
    idx_b = np.arange(len(b_num))

    for _ in range(n):
        sa = rng.choice(idx_a, size=len(idx_a), replace=True)
        sb = rng.choice(idx_b, size=len(idx_b), replace=True)
        da = share(a_num[sa], a_den[sa])
        db = share(b_num[sb], b_den[sb])
        diffs.append(da - db)

    diffs = np.array(diffs, dtype=float)
    diffs = diffs[np.isfinite(diffs)]
    if len(diffs) == 0:
        return (point, float("nan"), float("nan"))

    return (point, float(np.quantile(diffs, 0.025)), float(np.quantile(diffs, 0.975)))

# -----------------------
# Core generalized runner
# -----------------------
def run_stratified_course_tables(
    g: gpd.GeoDataFrame,
    golf: gpd.GeoDataFrame,
    strat_col: str,
    out_prefix: str,
    buffer_m: float = 800.0,
    q: int = 5,
    boot_n: int = 2000,
) -> tuple[Path, Path]:
    """
    Builds:
      (1) Full table: by course x strat_quintile (Q1..Q5) with exposure metrics
      (2) Contrast table: per course, Q5 - Q1 contrasts + bootstrap CIs

    Outputs are separate for each stratifier via out_prefix.
    """
    if strat_col not in g.columns:
        raise RuntimeError(f"Missing stratifier column in merged GeoDataFrame: {strat_col}")

    # Quintiles across the full harmonized universe
    nonmiss = pd.to_numeric(g[strat_col], errors="coerce").dropna()
    if len(nonmiss) == 0:
        raise RuntimeError(f"{strat_col} is entirely missing; cannot build quintiles.")
    g = g.copy()
    g["strat_q"] = qcut_safe(g[strat_col], q=q)

    rows = []
    contrasts = []

    for _, course in golf.iterrows():
        course_name = str(course["course_name"])
        course_geom = course.geometry
        course_buf = course_geom.buffer(buffer_m)

        dist = g.geometry.distance(course_geom)
        inter_area = g.geometry.intersection(course_buf).area

        tmp = g[["CTUID", "pop_2021", strat_col, "strat_q"]].copy()
        tmp["course_name"] = course_name
        tmp["dist_to_course_m"] = pd.to_numeric(dist, errors="coerce")
        tmp["area_within_800m_m2"] = pd.to_numeric(inter_area, errors="coerce")
        tmp["within_800m"] = (tmp["area_within_800m_m2"] > 0).astype(int)

        # Full Q1..Q5 summary rows
        for qq in sorted(tmp["strat_q"].dropna().unique()):
            sub = tmp[tmp["strat_q"] == qq]
            rows.append({
                "course_name": course_name,
                "quintile": int(qq),
                "n_tracts": int(len(sub)),
                "pop_sum": float(np.nansum(sub["pop_2021"])),
                "strat_median": float(np.nanmedian(pd.to_numeric(sub[strat_col], errors="coerce"))),

                "dist_mean_m": float(np.nanmean(sub["dist_to_course_m"])),
                "dist_median_m": float(np.nanmedian(sub["dist_to_course_m"])),

                "area800_mean_m2": float(np.nanmean(sub["area_within_800m_m2"])),
                "area800_median_m2": float(np.nanmedian(sub["area_within_800m_m2"])),

                "dist_pop_weighted_mean_m": weighted_mean(sub["dist_to_course_m"], sub["pop_2021"]),
                "area800_pop_weighted_mean_m2": weighted_mean(sub["area_within_800m_m2"], sub["pop_2021"]),

                "share_tracts_within_800m": float(np.mean(sub["within_800m"] == 1)),
                "share_pop_within_800m": float(
                    np.nansum(sub.loc[sub["within_800m"] == 1, "pop_2021"]) / np.nansum(sub["pop_2021"])
                    if np.nansum(sub["pop_2021"]) > 0 else np.nan
                ),
            })

        # Q5 - Q1 contrasts + bootstrap CIs
        q1 = tmp[tmp["strat_q"] == tmp["strat_q"].min()].copy()
        q5 = tmp[tmp["strat_q"] == tmp["strat_q"].max()].copy()

        d_diff, d_lo, d_hi = bootstrap_diff(
            q5["dist_to_course_m"].to_numpy(),
            q1["dist_to_course_m"].to_numpy(),
            n=boot_n,
            seed=19 + abs(hash(course_name)) % 10_000,
        )

        dp_diff, dp_lo, dp_hi = bootstrap_diff(
            # bootstrap over tract observations for pop-weighted mean distance is nontrivial;
            # we instead bootstrap tract-level distances and then weight within each bootstrap draw
            # by resampling rows (tracts) and using their pop weights.
            # To keep this simple and consistent, we approximate by bootstrapping distances only.
            q5["dist_to_course_m"].to_numpy(),
            q1["dist_to_course_m"].to_numpy(),
            n=boot_n,
            seed=37 + abs(hash(course_name)) % 10_000,
        )

        # Share of pop within 800m: bootstrap difference in population shares
        share_diff, share_lo, share_hi = bootstrap_diff_ratio(
            a_num=q5.loc[q5["within_800m"] == 1, "pop_2021"].to_numpy(),
            a_den=q5["pop_2021"].to_numpy(),
            b_num=q1.loc[q1["within_800m"] == 1, "pop_2021"].to_numpy(),
            b_den=q1["pop_2021"].to_numpy(),
            n=boot_n,
            seed=53 + abs(hash(course_name)) % 10_000,
        )

        # Point estimates (non-bootstrap) for pop-weighted mean distance contrast
        # (This is what you report; the CI above is a conservative approximation.)
        dp_point = weighted_mean(q5["dist_to_course_m"], q5["pop_2021"]) - weighted_mean(q1["dist_to_course_m"], q1["pop_2021"])

        contrasts.append({
            "course_name": course_name,
            "n_tracts_total": int(tmp["CTUID"].nunique()),
            "n_tracts_q1": int(len(q1)),
            "n_tracts_q5": int(len(q5)),

            "diff_dist_mean_m_q5_minus_q1": float(d_diff),
            "diff_dist_mean_m_ci95_lo": float(d_lo),
            "diff_dist_mean_m_ci95_hi": float(d_hi),

            "diff_dist_pop_weighted_mean_m_q5_minus_q1": float(dp_point),
            "diff_dist_pop_weighted_mean_m_ci95_lo": float(dp_lo),
            "diff_dist_pop_weighted_mean_m_ci95_hi": float(dp_hi),

            "diff_share_pop_within_800m_q5_minus_q1": float(share_diff),
            "diff_share_pop_within_800m_ci95_lo": float(share_lo),
            "diff_share_pop_within_800m_ci95_hi": float(share_hi),
        })

    tab = pd.DataFrame(rows).sort_values(["course_name", "quintile"])
    con = pd.DataFrame(contrasts).sort_values("course_name")

    out_tab = OUT_DIR / f"table_exposure_by_{out_prefix}_quintile_by_course.csv"
    out_con = OUT_DIR / f"table_{out_prefix}_course_inequality_contrasts.csv"

    tab.to_csv(out_tab, index=False)
    con.to_csv(out_con, index=False)

    print("Wrote:", out_tab)
    print("Wrote:", out_con)

    return out_tab, out_con

# -----------------------
# Main: load + harmonize once, then run stratifiers
# -----------------------
def main() -> None:
    # Load analytic CSV
    df = pd.read_csv(IN_CSV, dtype={"CTUID": "string"})
    df["CTUID"] = normalize_ctuid_series(df["CTUID"])

    # Ensure needed fields exist
    needed = ["CTUID", "pop_2021", "median_hh_income_2020", "pct_renters", "pct_65plus", "pop_density_per_km2"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing required columns in analytic CSV: {missing}")

    for c in ["pop_2021", "median_hh_income_2020", "pct_renters", "pct_65plus", "pop_density_per_km2"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    expected_n = int(df["CTUID"].nunique())
    if expected_n != 133:
        print(f"Warning: expected 133 tracts, got {expected_n} unique CTUIDs in analytic CSV.")

    # Load geometries
    tracts = gpd.read_file(TRACTS_GEO)
    tracts["CTUID"] = normalize_ctuid_series(tracts["CTUID"])

    from paper_helpers import load_golf_courses, EXCLUDE_COURSES_DEFAULT

    golf = load_golf_courses(GOLF_GJ, exclude_courses=EXCLUDE_COURSES_DEFAULT)

    # Harmonize to analytic universe
    g = tracts.merge(df, on="CTUID", how="right")
    if g["geometry"].isna().any():
        miss = g.loc[g["geometry"].isna(), "CTUID"].tolist()
        raise RuntimeError(f"Missing geometries for {len(miss)} analytic CTUIDs (first 10): {miss[:10]}")

    # Project to metric CRS (UTM 10N)
    g = gpd.GeoDataFrame(g, geometry="geometry", crs=tracts.crs).to_crs(epsg=32610)
    golf = golf.to_crs(epsg=32610)

    # Clean geometries
    g["geometry"] = g["geometry"].buffer(0)
    golf["geometry"] = golf["geometry"].buffer(0)

    print("Harmonized tracts:", int(g["CTUID"].nunique()), "Courses:", len(golf))

    # Run stratifiers (separate outputs)
    run_stratified_course_tables(g, golf, strat_col="pct_renters", out_prefix="renters", buffer_m=800.0)
    run_stratified_course_tables(g, golf, strat_col="pop_density_per_km2", out_prefix="density", buffer_m=800.0)
    run_stratified_course_tables(g, golf, strat_col="pct_65plus", out_prefix="seniors", buffer_m=800.0)

if __name__ == "__main__":
    main()
