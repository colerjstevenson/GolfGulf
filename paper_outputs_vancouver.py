from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent

IN_CSV = ROOT / "data/censusShape/vancouver/web_assets/metrics/_paper_analytic_tracts.csv"

OUT_DIR_T = ROOT / "outputs/paper_tables"
OUT_DIR_F = ROOT / "outputs/paper_figures"
OUT_DIR_T.mkdir(parents=True, exist_ok=True)
OUT_DIR_F.mkdir(parents=True, exist_ok=True)

# -----------------------
# Helpers
# -----------------------
def qcut_safe(s: pd.Series, q: int = 5) -> pd.Series:
    """
    Quintiles with safe fallback when many ties exist.
    """
    s2 = s.copy()
    # If too many missing, still return NA bins
    if s2.dropna().nunique() < q:
        # rank to break ties deterministically (average rank)
        r = s2.rank(method="average")
        return pd.qcut(r, q=q, labels=False, duplicates="drop") + 1
    return pd.qcut(s2, q=q, labels=False, duplicates="drop") + 1

def quintile_on_rank(series: pd.Series, q: int = 5) -> pd.Series:
    """
    Robust quintiles when the raw series has heaping/ties (common in rounded income).
    We compute quantiles on ranks, then return 1..q with NA preserved.
    """
    s = pd.to_numeric(series, errors="coerce")
    r = s.rank(method="average", na_option="keep")
    # qcut_safe returns labels 1..q on the provided (non-missing) index; reindex back
    return qcut_safe(r.dropna(), q=q).reindex(series.index)

def bootstrap_diff(a: np.ndarray, b: np.ndarray, n: int = 2000, seed: int = 7) -> tuple[float, float, float]:
    """
    Bootstrap CI for mean(a) - mean(b).
    Returns (diff, lo, hi) at 95%.
    """
    rng = np.random.default_rng(seed)
    a = a[np.isfinite(a)]
    b = b[np.isfinite(b)]
    if len(a) < 5 or len(b) < 5:
        return (float("nan"), float("nan"), float("nan"))
    diffs = []
    for _ in range(n):
        sa = rng.choice(a, size=len(a), replace=True)
        sb = rng.choice(b, size=len(b), replace=True)
        diffs.append(sa.mean() - sb.mean())
    diffs = np.array(diffs)
    return (float(a.mean() - b.mean()), float(np.quantile(diffs, 0.025)), float(np.quantile(diffs, 0.975)))

def weighted_mean(x: pd.Series, w: pd.Series) -> float:
    m = x.notna() & w.notna() & np.isfinite(x) & np.isfinite(w) & (w > 0)
    if m.sum() == 0:
        return float("nan")
    return float(np.average(x[m].astype(float), weights=w[m].astype(float)))

def ecdf(arr: np.ndarray):
    a = arr[np.isfinite(arr)]
    a = np.sort(a)
    y = np.arange(1, len(a) + 1) / len(a) if len(a) else np.array([])
    return a, y

# -----------------------
# Load
# -----------------------
df = pd.read_csv(IN_CSV, dtype={"CTUID": "string"})

# Quick sanity checks
required_cols = [
    "median_hh_income_2020", "pct_renters", "pct_65plus",
    "dist_to_golf_m", "golf_area_within_800m_m2", "pop_2021"
]
missing_cols = [c for c in required_cols if c not in df.columns]
if missing_cols:
    raise RuntimeError(f"Missing required columns in input CSV: {missing_cols}")

for c in required_cols:
    df[c] = pd.to_numeric(df[c], errors="coerce")

print("\n=== DIAGNOSTICS ===")
print("Rows:", len(df), "CTUID unique:", df["CTUID"].nunique())
for c in required_cols:
    s = df[c]
    print(f"{c}: missing={int(s.isna().sum())}/{len(s)} unique_nonmissing={int(s.dropna().nunique())}")
print("Income preview:")
print(df[["CTUID", "median_hh_income_2020"]].dropna().head(10).to_string(index=False))

# Basic sanity filters
df["median_hh_income_2020"] = pd.to_numeric(df["median_hh_income_2020"], errors="coerce")
df["pct_renters"] = pd.to_numeric(df["pct_renters"], errors="coerce")
df["pct_65plus"] = pd.to_numeric(df["pct_65plus"], errors="coerce")
df["dist_to_golf_m"] = pd.to_numeric(df["dist_to_golf_m"], errors="coerce")
df["golf_area_within_800m_m2"] = pd.to_numeric(df["golf_area_within_800m_m2"], errors="coerce")
df["pop_2021"] = pd.to_numeric(df["pop_2021"], errors="coerce")

# Diagnostics (so failures are self-explanatory)
print("\n=== DIAGNOSTICS ===")
for col in ["median_hh_income_2020", "pct_renters", "pct_65plus", "dist_to_golf_m", "golf_area_within_800m_m2", "pop_2021"]:
    s = df[col]
    print(f"{col}: n={len(s)}, missing={int(s.isna().sum())}, unique_nonmissing={int(s.dropna().nunique())}")
print("Income head (non-missing):")
print(df[["CTUID", "median_hh_income_2020"]].dropna().head(10).to_string(index=False))

# Quintiles (rank-based to handle heaping/ties cleanly)
df["income_q"]  = quintile_on_rank(df["median_hh_income_2020"], q=5)
df["renters_q"] = quintile_on_rank(df["pct_renters"], q=5)
df["seniors_q"] = quintile_on_rank(df["pct_65plus"], q=5)

# Density quintiles (if column exists; else leave as NA to avoid hard failure)
if "pop_density_per_km2" in df.columns:
    df["density_q"] = quintile_on_rank(df["pop_density_per_km2"], q=5)
else:
    df["density_q"] = pd.Series([pd.NA] * len(df), index=df.index, dtype="Int64")

# If income is all-missing, stop with a clear message
nonmiss_income = pd.to_numeric(df["median_hh_income_2020"], errors="coerce").dropna()
if len(nonmiss_income) == 0:
    raise RuntimeError(
        "median_hh_income_2020 is entirely missing in _paper_analytic_tracts.csv. "
        "Rebuild analytic dataset (income extraction)."
    )

# Optional one-time sanity check (leave in while debugging; remove later)
print("\nIncome quintile counts (incl NA):")
print(df["income_q"].value_counts(dropna=False).sort_index().to_string())

# -----------------------
# Table: exposure by income quintile
# -----------------------
rows = []
for q in sorted(df["income_q"].dropna().unique()):
    sub = df[df["income_q"] == q]
    rows.append({
        "income_quintile": int(q),
        "n_tracts": int(len(sub)),
        "pop_sum": float(np.nansum(sub["pop_2021"])),
        "income_median": float(np.nanmedian(sub["median_hh_income_2020"])),
        "dist_mean_m": float(np.nanmean(sub["dist_to_golf_m"])),
        "dist_median_m": float(np.nanmedian(sub["dist_to_golf_m"])),
        "golf_area800_mean_m2": float(np.nanmean(sub["golf_area_within_800m_m2"])),
        "golf_area800_median_m2": float(np.nanmedian(sub["golf_area_within_800m_m2"])),
        "dist_pop_weighted_mean_m": weighted_mean(sub["dist_to_golf_m"], sub["pop_2021"]),
        "golf_area800_pop_weighted_mean_m2": weighted_mean(sub["golf_area_within_800m_m2"], sub["pop_2021"]),
    })
tab_income = pd.DataFrame(rows).sort_values("income_quintile")

# Bootstrap: Q5 - Q1 differences (means)
q1 = df[df["income_q"] == tab_income["income_quintile"].min()]
q5 = df[df["income_q"] == tab_income["income_quintile"].max()]

dist_diff, dist_lo, dist_hi = bootstrap_diff(q5["dist_to_golf_m"].to_numpy(), q1["dist_to_golf_m"].to_numpy())
area_diff, area_lo, area_hi = bootstrap_diff(q5["golf_area_within_800m_m2"].to_numpy(), q1["golf_area_within_800m_m2"].to_numpy())

summary = pd.DataFrame([{
    "contrast": "Income Q5 minus Q1 (means, unweighted)",
    "dist_to_golf_m_diff": dist_diff,
    "dist_to_golf_m_ci95_lo": dist_lo,
    "dist_to_golf_m_ci95_hi": dist_hi,
    "golf_area800_m2_diff": area_diff,
    "golf_area800_m2_ci95_lo": area_lo,
    "golf_area800_m2_ci95_hi": area_hi
}])

tab_income.to_csv(OUT_DIR_T / "table_exposure_by_income_quintile.csv", index=False)
summary.to_csv(OUT_DIR_T / "table_exposure_summary.csv", index=False)

print("Wrote tables:",
      OUT_DIR_T / "table_exposure_by_income_quintile.csv",
      OUT_DIR_T / "table_exposure_summary.csv")

# -----------------------
# Figure 1: exposure gradient by income quintile (means + 95% bootstrap CI)
# -----------------------
means = []
cis_lo = []
cis_hi = []
qs = []
for q in tab_income["income_quintile"].tolist():
    sub = df[df["income_q"] == q]["dist_to_golf_m"].to_numpy()
    # bootstrap CI for mean
    rng = np.random.default_rng(11 + int(q))
    sub = sub[np.isfinite(sub)]
    if len(sub) < 5:
        means.append(np.nan); cis_lo.append(np.nan); cis_hi.append(np.nan); qs.append(q); continue
    boots = []
    for _ in range(2000):
        boots.append(rng.choice(sub, size=len(sub), replace=True).mean())
    boots = np.array(boots)
    means.append(sub.mean())
    cis_lo.append(np.quantile(boots, 0.025))
    cis_hi.append(np.quantile(boots, 0.975))
    qs.append(q)

fig_path1 = OUT_DIR_F / "fig_exposure_by_income_quintile.png"
plt.figure()
x = np.array(qs, dtype=float)
y = np.array(means, dtype=float)
yerr = np.vstack([y - np.array(cis_lo), np.array(cis_hi) - y])
plt.errorbar(x, y, yerr=yerr, fmt="o", capsize=4)
plt.xticks(x, [f"Q{int(v)}" for v in x])
plt.xlabel("Income quintile (tract median household income)")
plt.ylabel("Mean distance to nearest golf course (m)")
plt.title("Distance to nearest golf course by income quintile")
plt.tight_layout()
plt.savefig(fig_path1, dpi=200)
plt.close()

print("Wrote figure:", fig_path1)

# -----------------------
# Figure 2: ECDF of distance by income quintile (Q1 vs Q5)
# -----------------------
fig_path2 = OUT_DIR_F / "fig_distance_ecdf_by_income_quintile.png"
plt.figure()

for q, label in [(int(tab_income["income_quintile"].min()), "Q1 (lowest income)"),
                 (int(tab_income["income_quintile"].max()), "Q5 (highest income)")]:
    arr = df[df["income_q"] == q]["dist_to_golf_m"].to_numpy()
    xs, ys = ecdf(arr)
    plt.plot(xs, ys, label=label)

plt.xlabel("Distance to nearest golf course (m)")
plt.ylabel("ECDF")
plt.title("Distribution of distance to nearest golf course (Income Q1 vs Q5)")
plt.legend()
plt.tight_layout()
plt.savefig(fig_path2, dpi=200)
plt.close()

print("Wrote figure:", fig_path2)

# -----------------------
# Optional Figure 3: scatter (income vs distance) with log income axis
# -----------------------
fig_path3 = OUT_DIR_F / "fig_distance_vs_income_scatter.png"
sub = df[["median_hh_income_2020", "dist_to_golf_m"]].dropna()
plt.figure()
plt.scatter(sub["median_hh_income_2020"], sub["dist_to_golf_m"], s=10, alpha=0.6)
plt.xscale("log")
plt.xlabel("Median household income (log scale)")
plt.ylabel("Distance to nearest golf course (m)")
plt.title("Distance to golf vs tract median household income")
plt.tight_layout()
plt.savefig(fig_path3, dpi=200)
plt.close()

print("Wrote figure:", fig_path3)
