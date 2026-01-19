# paper_make_static_maps_vancouver.py
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import geopandas as gpd

import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
from matplotlib.patches import Patch

# ---------------------------------------------------------------------
# Paths / config
# ---------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent

IN_CSV = ROOT / "data/censusShape/vancouver/web_assets/metrics/_paper_analytic_tracts.csv"
TRACTS_GEO = ROOT / "data/censusShape/vancouver/web_assets/tracts.geojson"
GOLF_GJ = ROOT / "data/censusShape/vancouver/web_assets/golf_courses.geojson"

OUT_DIR = ROOT / "outputs/paper_figures/maps"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Exclusions for map overlays (consistent with tables/figures)
EXCLUDE_COURSES = {"Stanley Park Pitch & Putt"}

# Vancouver in meters
TARGET_EPSG = 32610
BUFFER_M = 800.0

# Typography
mpl.rcParams["font.family"] = "Helvetica"
mpl.rcParams["axes.titlesize"] = 12
mpl.rcParams["axes.titleweight"] = "regular"

# ---------------------------------------------------------------------
# Utility: CTUID normalization
# ---------------------------------------------------------------------
def normalize_ctuid(obj) -> str | None:
    if pd.isna(obj):
        return None
    s = str(obj).strip()
    # strip common geojson float artifacts
    if s.endswith(".0"):
        s = s[:-2]
    if s.endswith(".00"):
        s = s[:-3]
    return s


# ---------------------------------------------------------------------
# Quintiles + reportable cutpoints (ranges for legend)
# ---------------------------------------------------------------------
def compute_quintiles_with_edges(s: pd.Series, q: int = 5) -> tuple[pd.Series, list[float]]:
    """
    Returns:
      - qcat: Int64 labels 1..k (k may be < q if quantile edges collapse due to ties)
      - edges: numeric bin edges in original units (len = k+1)

    Uses explicit quantile cutpoints on original values (reportable in legend).
    """
    s2 = pd.to_numeric(s, errors="coerce")
    vals = s2.dropna()
    if vals.empty:
        return pd.Series([pd.NA] * len(s2), index=s2.index, dtype="Int64"), []

    probs = np.linspace(0, 1, q + 1)
    raw_edges = vals.quantile(probs).to_numpy(dtype=float)

    # Remove duplicates to avoid non-increasing bins
    edges = np.unique(raw_edges)
    if len(edges) < 2:
        return pd.Series([pd.NA] * len(s2), index=s2.index, dtype="Int64"), []

    # Bin into 1..k
    qbin = pd.cut(s2, bins=edges, include_lowest=True, labels=False)
    qcat = (qbin + 1).astype("Int64")

    return qcat, edges.tolist()


def format_edges_as_labels(edges: list[float], *, kind: str) -> list[str]:
    """
    Convert bin edges into readable legend labels.

    kind:
      - 'income'   -> $xx,xxx
      - 'percent'  -> x.x%
      - 'count'    -> x,xxx
      - 'density'  -> x,xxx (per km²)
    """
    if not edges or len(edges) < 2:
        return []

    def fmt(x: float) -> str:
        if x is None or not np.isfinite(x):
            return "NA"
        if kind == "income":
            return f"${x:,.0f}"
        if kind == "percent":
            return f"{x:.1f}%"
        if kind == "density":
            return f"{x:,.0f}"
        return f"{x:,.0f}"

    labels: list[str] = []
    for i in range(len(edges) - 1):
        lo = edges[i]
        hi = edges[i + 1]
        labels.append(f"{i+1}: {fmt(lo)}–{fmt(hi)}")
    return labels


# ---------------------------------------------------------------------
# Colormap: fixed 5-class palette (Q1->Q5)
# ---------------------------------------------------------------------
def greens_quintiles_5() -> ListedColormap:
    """
    Custom 5-class sequential palette for ordered quintiles (light -> dark).
    Q1..Q5 are mapped to these hex colors exactly.
    """
    colors = [
        "#edf8e9",  # Q1
        "#bae4b3",  # Q2
        "#74c476",  # Q3
        "#31a354",  # Q4
        "#006d2c",  # Q5
    ]
    return ListedColormap(colors, name="greens_q5")


# ---------------------------------------------------------------------
# Plot helpers
# ---------------------------------------------------------------------
def add_scalebar_like_label(ax, gdf: gpd.GeoDataFrame) -> None:
    """
    Simple scale indicator anchored in data coordinates.
    Assumes projected CRS in meters.
    """
    xmin, ymin, xmax, ymax = gdf.total_bounds
    width_m = xmax - xmin
    height_m = ymax - ymin
    if not np.isfinite(width_m) or width_m <= 0 or not np.isfinite(height_m) or height_m <= 0:
        return

    target = 0.25 * width_m
    nice = _nice_round_meters(target)
    if nice <= 0:
        return

    pad_x = 0.04 * width_m
    pad_y = 0.04 * height_m
    x0 = xmin + pad_x
    y0 = ymin + pad_y
    x1 = x0 + nice

    tick = 0.008 * height_m

    ax.plot([x0, x1], [y0, y0], linewidth=2.0, color="black", zorder=20)
    ax.plot([x0, x0], [y0 - tick, y0 + tick], linewidth=2.0, color="black", zorder=20)
    ax.plot([x1, x1], [y0 - tick, y0 + tick], linewidth=2.0, color="black", zorder=20)

    if nice >= 1000:
        label = f"{nice/1000:.0f} km"
    else:
        label = f"{nice:.0f} m"

    ax.text(
        (x0 + x1) / 2,
        y0 + 0.012 * height_m,
        label,
        ha="center",
        va="bottom",
        fontsize=9,
        color="black",
        fontfamily="Helvetica",
        zorder=21,
    )


def _nice_round_meters(x: float) -> float:
    """
    Round to 1/2/5 * 10^k meters.
    """
    if not np.isfinite(x) or x <= 0:
        return 0.0
    k = 10 ** np.floor(np.log10(x))
    r = x / k
    if r <= 1:
        return 1 * k
    if r <= 2:
        return 2 * k
    if r <= 5:
        return 5 * k
    return 10 * k


def make_quintile_legend(
    ax,
    cmap: ListedColormap,
    *,
    title: str,
    labels: list[str],
    include_missing: bool,
    missing_color: str = "lightgrey",
) -> None:
    """
    Legend is placed outside the map (right side), with range labels and course types.
    """
    from matplotlib.lines import Line2D
    
    handles: list = []
    # Use as many labels as we actually have bins (usually 5; can be fewer if edges collapse)
    for i, lab in enumerate(labels):
        handles.append(Patch(facecolor=cmap(i), edgecolor="none", label=lab))
    if include_missing:
        handles.append(Patch(facecolor=missing_color, edgecolor="none", label="Missing"))
    
    # Add course type legend entries
    handles.append(Line2D([0], [0], color="#d62728", linewidth=2, label="Private"))
    handles.append(Line2D([0], [0], color="#9467bd", linewidth=2, label="Municipal"))
    handles.append(Line2D([0], [0], color="#1f77b4", linewidth=2, label="Public"))

    leg = ax.legend(
        handles=handles,
        title=title,
        frameon=True,
        loc="center left",
        bbox_to_anchor=(1.02, 0.5),  # outside to the right
        borderaxespad=0.0,
        handlelength=1.2,
        handleheight=1.2,
        labelspacing=0.6,
        framealpha=0.95,
        fancybox=True,
        shadow=True,
        fontsize=9,
    )
    leg.get_frame().set_linewidth(1.0)
    leg.get_frame().set_edgecolor("#cccccc")
    for t in leg.get_texts():
        t.set_fontfamily("Helvetica")
        t.set_fontsize(9)
    leg.get_title().set_fontfamily("Helvetica")
    leg.get_title().set_fontsize(10)
    leg.get_title().set_fontweight("bold")


def make_quintile_map(
    tracts: gpd.GeoDataFrame,
    golf: gpd.GeoDataFrame,
    *,
    qcol: str,
    title: str,
    out_stub: str,
    cmap: ListedColormap,
    legend_labels: list[str],
    buffer_m: float = 800.0,
    show_buffers: bool = True,
    show_courses: bool = True,
    annotate_courses: bool = False,
    legend_title: str = "Quintile range",
) -> None:
    if qcol not in tracts.columns:
        raise RuntimeError(f"Missing quintile column: {qcol}")

    # Filter golf courses to only those that intersect tracts, plus University Golf Club
    tracts_union = tracts.unary_union
    golf_filtered = golf[
        golf.geometry.intersects(tracts_union) | 
        (golf["course_name"].str.strip() == "University Golf Club")
    ].copy()

    fig, ax = plt.subplots(figsize=(10, 9))  # larger for better visibility
    ax.set_aspect("equal")
    ax.axis("off")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=20)

    miss = tracts[tracts[qcol].isna()].copy()
    ok = tracts[tracts[qcol].notna()].copy()

    # Base fills
    if len(miss) > 0:
        miss.plot(ax=ax, color="lightgrey", linewidth=0.0, edgecolor="none", zorder=1)

    if len(ok) > 0:
        ok[qcol] = ok[qcol].astype(int)
        ok["_q0"] = ok[qcol] - 1  # 1..k -> 0..k-1
        ok.plot(
            ax=ax,
            column="_q0",
            cmap=cmap,
            linewidth=0.0,
            edgecolor="none",
            zorder=2,
        )

    # Plot course outlines by type - using filtered courses
        # Color code by course access type
        access_colors = {"Private": "#d62728", "Municipal": "#9467bd", "Public": "#1f77b4"}
        
        if "access" in golf_filtered.columns:
            for access_type, color in access_colors.items():
                type_subset = golf_filtered[golf_filtered["access"].str.strip() == access_type]
                if len(type_subset) > 0:
                    # Plot course outlines with translucent fill
                    type_subset.plot(ax=ax, facecolor=color, edgecolor=color, linewidth=1.5, alpha=0.7, zorder=4)
            
            # Plot any courses with unmatched access types (NOMATCH, etc.) in gray
            matched = golf_filtered["access"].str.strip().isin(access_colors.keys())
            unmatched = golf_filtered[~matched]
            if len(unmatched) > 0:
                unmatched.plot(ax=ax, facecolor="gray", edgecolor="gray", linewidth=1.5, alpha=0.7, zorder=4)
        else:
            # Fallback to black if no access column
            golf_filtered.plot(ax=ax, facecolor="black", edgecolor="black", linewidth=1.5, alpha=0.7, zorder=4)

    # Tract boundaries LAST (so they are visible)
    tracts.boundary.plot(ax=ax, linewidth=0.6, edgecolor="black", zorder=10)

    if annotate_courses and len(golf_filtered) > 0:
        for _, row in golf_filtered.iterrows():
            pt = row.geometry.representative_point()
            ax.text(pt.x, pt.y, str(row["course_name"]), fontsize=7, fontfamily="Helvetica", zorder=11)

    # Set axis limits to tract bounds with buffer, removing outliers
    # Use convex hull to remove isolated tracts, then add 5% padding
    from shapely.geometry import MultiPolygon
    
    hull = tracts.unary_union.convex_hull
    xmin, ymin, xmax, ymax = hull.bounds
    width = xmax - xmin
    height = ymax - ymin
    pad = 0.05  # 5% padding
    
    xmin -= width * pad
    xmax += width * pad
    ymin -= height * pad
    ymax += height * pad
    
    ax.set_xlim(xmin, xmax)
    ax.set_ylim(ymin, ymax)

    make_quintile_legend(
        ax,
        cmap,
        title=legend_title,
        labels=legend_labels,
        include_missing=(len(miss) > 0),
        missing_color="lightgrey",
    )
    add_scalebar_like_label(ax, tracts)

    # Leave room for legend on the right
    fig.tight_layout(rect=[0, 0, 0.78, 0.96])

    png_path = OUT_DIR / f"{out_stub}.png"
    pdf_path = OUT_DIR / f"{out_stub}.pdf"
    fig.savefig(png_path, dpi=300, bbox_inches="tight")
    fig.savefig(pdf_path, bbox_inches="tight")
    plt.close(fig)

    print(f"Wrote: {png_path}")
    print(f"Wrote: {pdf_path}")


# ---------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------
def load_tracts_and_metrics() -> gpd.GeoDataFrame:
    df = pd.read_csv(IN_CSV, dtype={"CTUID": "string"})
    if "CTUID" not in df.columns:
        raise RuntimeError(f"IN_CSV missing CTUID column: {IN_CSV}")
    df["CTUID_key"] = df["CTUID"].map(normalize_ctuid).astype("string")

    tracts = gpd.read_file(TRACTS_GEO)
    if "CTUID" not in tracts.columns:
        raise RuntimeError(f"TRACTS_GEO missing CTUID column: {TRACTS_GEO}")
    tracts["CTUID_key"] = tracts["CTUID"].map(normalize_ctuid).astype("string")

    # Harmonize to analytic universe
    g = tracts.merge(df, on="CTUID_key", how="right", suffixes=("_geo", ""))
    n_missing = int(g["geometry"].isna().sum())
    if n_missing > 0:
        miss = g.loc[g["geometry"].isna(), "CTUID_key"].astype("string").tolist()[:20]
        raise RuntimeError(f"Missing {n_missing} tract geometries after merge. Examples: {miss}")

    g = gpd.GeoDataFrame(g, geometry="geometry", crs=tracts.crs)
    g = g.to_crs(epsg=TARGET_EPSG)
    g["geometry"] = g["geometry"].buffer(0)

    # Numeric coercions commonly used
    for c in [
        "median_hh_income_2020",
        "pct_renters",
        "pct_65plus",
        "pop_2021",
        "land_area_km2",
        "area_km2",
    ]:
        if c in g.columns:
            g[c] = pd.to_numeric(g[c], errors="coerce")

    # Density (if not already present)
    if "pop_density_per_km2" not in g.columns:
        area_col = None
        if "land_area_km2" in g.columns:
            area_col = "land_area_km2"
        elif "area_km2" in g.columns:
            area_col = "area_km2"

        if area_col is not None and "pop_2021" in g.columns:
            area = pd.to_numeric(g[area_col], errors="coerce")
            pop = pd.to_numeric(g["pop_2021"], errors="coerce")
            with np.errstate(divide="ignore", invalid="ignore"):
                g["pop_density_per_km2"] = pop / area
        else:
            g["pop_density_per_km2"] = pd.NA

    return g


def load_golf_courses() -> gpd.GeoDataFrame:
    golf = gpd.read_file(GOLF_GJ)

    # golf geojson uses 'name'; standardize to 'course_name'
    if "course_name" not in golf.columns and "name" in golf.columns:
        golf = golf.rename(columns={"name": "course_name"})

    if "course_name" not in golf.columns:
        raise RuntimeError(f"Golf GeoJSON missing course_name/name column. Found: {list(golf.columns)}")

    golf["course_name"] = (
        golf["course_name"]
        .astype("string")
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    golf = golf[~golf["course_name"].isin(EXCLUDE_COURSES)].copy()
    golf = gpd.GeoDataFrame(golf, geometry="geometry", crs=golf.crs)
    golf = golf.to_crs(epsg=TARGET_EPSG)
    golf["geometry"] = golf["geometry"].buffer(0)

    return golf.reset_index(drop=True)


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main() -> None:
    g = load_tracts_and_metrics()
    golf = load_golf_courses()

    print("\n=== MAP INPUT CHECK ===")
    print("Tracts:", len(g), "CTUID unique:", g["CTUID_key"].nunique())
    print("Courses:", len(golf))
    if len(golf) == 0:
        print("[WARN] No courses loaded after exclusions; maps will have no overlays.")

    cmap = greens_quintiles_5()

    # (vcol, title, stub, legend_kind)
    var_specs = [
        ("median_hh_income_2020", "Vancouver census tracts: Median household income", "map_income_quintiles", "income"),
        ("pct_renters", "Vancouver census tracts: Renter households (%)", "map_renters_quintiles", "percent"),
        ("pct_65plus", "Vancouver census tracts: Population aged 65+ (%)", "map_seniors_quintiles", "percent"),
        ("pop_2021", "Vancouver census tracts: Population (2021)", "map_population_quintiles", "count"),
        ("pop_density_per_km2", "Vancouver census tracts: Population density (per km²)", "map_density_quintiles", "density"),
    ]

    for vcol, title, stub, legend_kind in var_specs:
        if vcol not in g.columns:
            print(f"[SKIP] Missing column: {vcol}")
            continue

        qcol = f"{vcol}_q"
        g[qcol], edges = compute_quintiles_with_edges(g[vcol], q=5)
        legend_labels = format_edges_as_labels(edges, kind=legend_kind)

        if not legend_labels:
            legend_labels = [str(i) for i in range(1, 6)]

        make_quintile_map(
            g,
            golf,
            qcol=qcol,
            title=title,
            out_stub=stub,
            cmap=cmap,
            legend_labels=legend_labels,
            buffer_m=BUFFER_M,
            show_buffers=True,
            show_courses=True,
            annotate_courses=False,
            legend_title="Quintile range",
        )

    print("\n=== DONE ===")
    print(f"Maps written to: {OUT_DIR}")


if __name__ == "__main__":
    main()
