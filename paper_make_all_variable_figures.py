# paper_make_all_variable_figures.py
from __future__ import annotations

from pathlib import Path
from typing import Optional, Iterable

import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter


# ----------------------------
# Paths
# ----------------------------
ROOT = Path(__file__).resolve().parent
TABLES = ROOT / "outputs" / "paper_tables"
FIGS = ROOT / "outputs" / "paper_figures"
FIGS.mkdir(parents=True, exist_ok=True)

# ----------------------------
# Style
# ----------------------------
mpl.rcParams.update(
    {
        "font.family": "sans-serif",
        "font.sans-serif": ["Helvetica", "Arial", "DejaVu Sans"],
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
        "axes.titlesize": 12,
        "axes.labelsize": 11,
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,
        "figure.dpi": 300,
        "savefig.dpi": 300,
    }
)

OKABE_ITO = {
    "black": "#000000",
    "orange": "#E69F00",
    "skyblue": "#56B4E9",
    "bluishgreen": "#009E73",
    "yellow": "#F0E442",
    "blue": "#0072B2",
    "vermillion": "#D55E00",
    "reddishpurple": "#CC79A7",
}

# Discrete viridis (5)
VIRIDIS5 = [mpl.cm.get_cmap("viridis")(x) for x in np.linspace(0.15, 0.90, 5)]


def meters_to_km_formatter(x, pos) -> str:
    if x >= 1000:
        return f"{x/1000:.1f} km"
    return f"{int(x):d} m"


def ensure_numeric(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


# ----------------------------
# Reusable forest plot
# ----------------------------
def forest_plot(
    df: pd.DataFrame,
    *,
    course_col: str,
    effect_col: str,
    ci_lo_col: str,
    ci_hi_col: str,
    title: str,
    x_label: str,
    out_stub: Path,
    color: str,
    sort: str = "effect",  # effect or alphabetical
    zero_line: float = 0.0,
) -> None:
    for c in [course_col, effect_col, ci_lo_col, ci_hi_col]:
        if c not in df.columns:
            raise KeyError(f"Missing '{c}' in columns: {list(df.columns)}")

    d = df[[course_col, effect_col, ci_lo_col, ci_hi_col]].copy()
    d = ensure_numeric(d, [effect_col, ci_lo_col, ci_hi_col]).dropna(subset=[effect_col])

    if sort == "effect":
        d = d.sort_values(effect_col, ascending=True)
    elif sort == "alphabetical":
        d = d.sort_values(course_col, ascending=True)
    else:
        raise ValueError("sort must be 'effect' or 'alphabetical'")

    n = len(d)
    if n == 0:
        raise RuntimeError("No non-missing rows to plot.")

    height = max(3.5, 0.38 * n + 1.4)
    fig, ax = plt.subplots(figsize=(7.2, height))

    y = np.arange(n)
    x = d[effect_col].to_numpy(dtype=float)
    lo = d[ci_lo_col].to_numpy(dtype=float)
    hi = d[ci_hi_col].to_numpy(dtype=float)

    ax.hlines(y=y, xmin=lo, xmax=hi, color=color, lw=2.0, alpha=0.95)
    ax.scatter(x, y, s=34, color=color, zorder=3)
    ax.axvline(zero_line, color=OKABE_ITO["black"], lw=1.0, alpha=0.6)

    ax.set_yticks(y)
    ax.set_yticklabels(d[course_col].astype(str).tolist())
    ax.set_xlabel(x_label)
    ax.set_title(title)

    ax.xaxis.set_major_formatter(FuncFormatter(meters_to_km_formatter))

    finite = np.isfinite(np.r_[lo, hi, x])
    if finite.any():
        mn = float(np.nanmin(np.r_[lo, hi, x]))
        mx = float(np.nanmax(np.r_[lo, hi, x]))
        pad = (mx - mn) * 0.08 if mx > mn else 1.0
        ax.set_xlim(mn - pad, mx + pad)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="x", linestyle=":", linewidth=0.8, alpha=0.35)

    fig.tight_layout()
    fig.savefig(out_stub.with_suffix(".png"), bbox_inches="tight")
    fig.savefig(out_stub.with_suffix(".pdf"), bbox_inches="tight")
    plt.close(fig)


# ----------------------------
# Heatmap: course × quintile for a metric
# ----------------------------
def heatmap_course_by_quintile(
    df: pd.DataFrame,
    *,
    course_col: str,
    quintile_col: str,
    value_col: str,
    title: str,
    cbar_label: str,
    out_stub: Path,
    cmap: str = "viridis",
    value_is_meters: bool = False,
) -> None:
    for c in [course_col, quintile_col, value_col]:
        if c not in df.columns:
            raise KeyError(f"Missing '{c}' in columns: {list(df.columns)}")

    d = df[[course_col, quintile_col, value_col]].copy()
    d[quintile_col] = pd.to_numeric(d[quintile_col], errors="coerce")
    d[value_col] = pd.to_numeric(d[value_col], errors="coerce")
    d = d.dropna(subset=[course_col, quintile_col, value_col])

    piv = d.pivot_table(
        index=course_col,
        columns=quintile_col,
        values=value_col,
        aggfunc="mean",
    )

    # Ensure Q1..Q5 order (if available)
    cols = [c for c in [1, 2, 3, 4, 5] if c in piv.columns]
    piv = piv[cols]

    fig, ax = plt.subplots(figsize=(8.2, max(3.8, 0.35 * piv.shape[0] + 1.8)))
    im = ax.imshow(piv.to_numpy(), aspect="auto", interpolation="nearest", cmap=cmap)

    ax.set_title(title)
    ax.set_xlabel("Quintile")
    ax.set_ylabel("Course")

    ax.set_xticks(np.arange(len(piv.columns)))
    ax.set_xticklabels([f"Q{int(c)}" for c in piv.columns])

    ax.set_yticks(np.arange(len(piv.index)))
    ax.set_yticklabels([str(x) for x in piv.index])

    cbar = fig.colorbar(im, ax=ax, shrink=0.92, pad=0.02)
    cbar.set_label(cbar_label)

    if value_is_meters:
        cbar.ax.yaxis.set_major_formatter(FuncFormatter(meters_to_km_formatter))

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.tight_layout()
    fig.savefig(out_stub.with_suffix(".png"), bbox_inches="tight")
    fig.savefig(out_stub.with_suffix(".pdf"), bbox_inches="tight")
    plt.close(fig)


# ----------------------------
# Batch: contrasts → forest plots
# ----------------------------
def generate_all_forest_plots() -> None:
    """
    Looks for:
      - table_course_inequality_contrasts.csv
      - table_*_course_inequality_contrasts.csv  (renters/seniors/density)
    Produces up to 3 forest plots per variable (if columns exist):
      - diff_dist_mean_m_q5_minus_q1 (+ CI)
      - diff_dist_pop_weighted_mean_m_q5_minus_q1 (+ CI)
      - diff_share_pop_within_800m_q5_minus_q1 (+ CI)  [often missing in your tables]
    """
    contrast_files = sorted(TABLES.glob("table_*course_inequality_contrasts.csv"))
    if not contrast_files:
        raise RuntimeError(f"No contrasts tables found in {TABLES}")

    for fp in contrast_files:
        df = pd.read_csv(fp)
        course_col = "course_name"

        # Infer variable label from filename
        # - table_course_inequality_contrasts.csv -> income (your income table uses this name)
        # - table_renters_course_inequality_contrasts.csv -> renters, etc.
        stem = fp.stem
        if stem == "table_course_inequality_contrasts":
            var = "income"
        else:
            var = stem.replace("table_", "").replace("_course_inequality_contrasts", "")

        # 1) Tract-weighted mean distance contrast
        needed = [
            "diff_dist_mean_m_q5_minus_q1",
            "diff_dist_mean_m_ci95_lo",
            "diff_dist_mean_m_ci95_hi",
        ]
        if all(c in df.columns for c in needed):
            forest_plot(
                df,
                course_col=course_col,
                effect_col=needed[0],
                ci_lo_col=needed[1],
                ci_hi_col=needed[2],
                title=f"Difference in mean distance (Q5 − Q1) by course: {var}",
                x_label="Mean distance difference to course (Q5 − Q1)",
                out_stub=FIGS / f"fig_forest_{var}_dist_mean_q5_minus_q1",
                color=OKABE_ITO["blue"],
            )

        # 2) Population-weighted mean distance contrast
        needed = [
            "diff_dist_pop_weighted_mean_m_q5_minus_q1",
            "diff_dist_pop_weighted_mean_m_ci95_lo",
            "diff_dist_pop_weighted_mean_m_ci95_hi",
        ]
        if all(c in df.columns for c in needed):
            forest_plot(
                df,
                course_col=course_col,
                effect_col=needed[0],
                ci_lo_col=needed[1],
                ci_hi_col=needed[2],
                title=f"Difference in population-weighted mean distance (Q5 − Q1) by course: {var}",
                x_label="Population-weighted mean distance difference to course (Q5 − Q1)",
                out_stub=FIGS / f"fig_forest_{var}_dist_popw_q5_minus_q1",
                color=OKABE_ITO["vermillion"],
            )

        # 3) Share of population within 800m contrast (often absent/empty)
        needed = [
            "diff_share_pop_within_800m_q5_minus_q1",
            "diff_share_pop_within_800m_ci95_lo",
            "diff_share_pop_within_800m_ci95_hi",
        ]
        if all(c in df.columns for c in needed):
            # If the column exists but is all-missing, skip cleanly
            tmp = ensure_numeric(df.copy(), needed)
            if tmp[needed[0]].notna().any():
                forest_plot(
                    tmp,
                    course_col=course_col,
                    effect_col=needed[0],
                    ci_lo_col=needed[1],
                    ci_hi_col=needed[2],
                    title=f"Difference in population share within 800 m (Q5 − Q1) by course: {var}",
                    x_label="Population share difference within 800 m (Q5 − Q1)",
                    out_stub=FIGS / f"fig_forest_{var}_share_pop_within800_q5_minus_q1",
                    color=OKABE_ITO["bluishgreen"],
                )


# ----------------------------
# Batch: exposure-by-course → heatmaps
# ----------------------------
def generate_all_heatmaps() -> None:
    """
    Looks for:
      - table_exposure_by_*_quintile_by_course.csv
    Produces heatmaps for:
      - dist_mean_m
      - dist_pop_weighted_mean_m
      - share_pop_within_800m (if present)
    """
    exposure_files = sorted(TABLES.glob("table_exposure_by_*_quintile_by_course.csv"))
    if not exposure_files:
        raise RuntimeError(f"No exposure-by-course tables found in {TABLES}")

    for fp in exposure_files:
        df = pd.read_csv(fp)

        # variable label from filename
        # table_exposure_by_income_quintile_by_course -> income
        var = fp.stem.replace("table_exposure_by_", "").replace("_quintile_by_course", "")

        # Identify likely columns
        course_col = "course_name"
        quintile_col = None
        for cand in ["income_quintile", "renters_quintile", "seniors_quintile", "density_quintile", "quintile"]:
            if cand in df.columns:
                quintile_col = cand
                break
        if quintile_col is None:
            # In your current tables, the quintile column appears to just be an integer column
            # in position 2; but we do not guess. If this triggers, print cols and fix once.
            raise KeyError(f"Cannot find a quintile column in {fp.name}. Columns: {list(df.columns)}")

        # Distance (tract mean)
        if "dist_mean_m" in df.columns:
            heatmap_course_by_quintile(
                df,
                course_col=course_col,
                quintile_col=quintile_col,
                value_col="dist_mean_m",
                title=f"Mean distance to course by quintile and course: {var}",
                cbar_label="Mean distance",
                out_stub=FIGS / f"fig_heat_{var}_dist_mean_by_course_quintile",
                cmap="viridis",
                value_is_meters=True,
            )

        # Distance (population-weighted mean)
        if "dist_pop_weighted_mean_m" in df.columns:
            heatmap_course_by_quintile(
                df,
                course_col=course_col,
                quintile_col=quintile_col,
                value_col="dist_pop_weighted_mean_m",
                title=f"Population-weighted mean distance to course by quintile and course: {var}",
                cbar_label="Population-weighted mean distance",
                out_stub=FIGS / f"fig_heat_{var}_dist_popw_by_course_quintile",
                cmap="viridis",
                value_is_meters=True,
            )

        # Share pop within 800m (if present)
        if "share_pop_within_800m" in df.columns:
            tmp = df.copy()
            tmp["share_pop_within_800m"] = pd.to_numeric(tmp["share_pop_within_800m"], errors="coerce")
            if tmp["share_pop_within_800m"].notna().any():
                heatmap_course_by_quintile(
                    tmp,
                    course_col=course_col,
                    quintile_col=quintile_col,
                    value_col="share_pop_within_800m",
                    title=f"Population within 800 m of course by quintile and course: {var}",
                    cbar_label="Population share within 800 m",
                    out_stub=FIGS / f"fig_heat_{var}_share_pop_within800_by_course_quintile",
                    cmap="viridis",
                    value_is_meters=False,
                )


def main() -> None:
    generate_all_forest_plots()
    generate_all_heatmaps()
    print(f"Done. Wrote figures to: {FIGS}")


if __name__ == "__main__":
    main()
