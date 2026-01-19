#!/usr/bin/env python
"""
Census Summary by Golf Course Access Type

Computes population-weighted census statistics around Vancouver golf courses
grouped by access type (Private, Municipal, Public) and citywide average.

Uses 10 largest golf courses from OSM, buffers each by 800m, and overlays
with census tracts to compute weighted averages of demographic metrics.

Output:
- course_census_summary.png: Transposed table with metrics as rows,
  access types as columns
"""

from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import osmnx as ox

# ============================================================================
# PATHS & SETTINGS
# ============================================================================

ROOT = Path(__file__).resolve().parent
IN_CSV = ROOT / "data/censusShape/vancouver/web_assets/metrics/_paper_analytic_tracts.csv"
TRACTS_GEO = ROOT / "data/censusShape/vancouver/web_assets/tracts.geojson"
GOLF_GJ = ROOT / "data/censusShape/vancouver/web_assets/golf_courses.geojson"
OUTPUT_DIR = ROOT / "analysis_output/golf_census_analysis"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TARGET_EPSG = 32610
BUFFER_M = 800.0
COURSE_MIN_HA = 1.0
ACCESS_TYPES = ["Private", "Municipal", "Public"]

ROW_COLORS = {
    "Private": "#f4d4d4",
    "Municipal": "#e5d9f2",
    "Public": "#d7e9fb",
    "City Average": "#e9ecef",
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def normalize_ctuid(val: str | None) -> str | None:
    """Normalize CTUID for joining."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.endswith(".00"):
        s = s[:-3]
    return s


def classify_access(val: str | None) -> str:
    """Classify golf course access type."""
    if pd.isna(val):
        return "NOMATCH"
    s = str(val).lower()
    if "private" in s:
        return "Private"
    if "municipal" in s:
        return "Municipal"
    if "public" in s:
        return "Public"
    return "NOMATCH"


def load_tracts_with_metrics() -> gpd.GeoDataFrame:
    """Load census tracts with metrics."""
    tracts = gpd.read_file(TRACTS_GEO).to_crs(epsg=TARGET_EPSG)
    metrics = pd.read_csv(IN_CSV)
    
    tracts["CTUID"] = tracts.get("CTUID", tracts.get("ct_uid", "")).apply(normalize_ctuid)
    metrics["CTUID"] = metrics.get("CTUID", metrics.get("ct_uid", "")).apply(normalize_ctuid)
    
    tracts = tracts.merge(metrics, on="CTUID", how="left")
    
    for col in ["median_hh_income_2020", "pct_renters", "pct_65plus", "pop_2021", "pop_density_per_km2"]:
        if col in tracts.columns:
            tracts[col] = pd.to_numeric(tracts[col], errors="coerce")
    
    if "pop_density_per_km2" not in tracts.columns or tracts["pop_density_per_km2"].isna().all():
        tracts["pop_density_per_km2"] = tracts["pop_2021"] / (tracts.geometry.area / 1e6)
    
    return tracts


def fetch_golf_courses() -> gpd.GeoDataFrame:
    """Load and filter golf courses."""
    if not GOLF_GJ.exists():
        return gpd.GeoDataFrame()
    
    g = gpd.read_file(GOLF_GJ).to_crs(epsg=TARGET_EPSG)
    g["area_ha"] = g.geometry.area / 10_000
    g = g[g["area_ha"] >= COURSE_MIN_HA].copy()
    
    g["course_name"] = g.get("name", "Unknown").fillna("Unknown").astype(str).str.strip()
    g["access_type"] = g.get("access", pd.Series([None] * len(g))).apply(classify_access)
    g.loc[g["access_type"] == "NOMATCH", "access_type"] = "Public"
    g = g[g["access_type"].isin(ACCESS_TYPES)].copy()
    
    g_wgs = g.to_crs(epsg=4326)
    bbox = g_wgs.bounds
    bbox_mask = (bbox.minx > -123.35) & (bbox.maxx < -123.00) & (bbox.miny > 49.15) & (bbox.maxy < 49.40)
    g = g.loc[bbox_mask].copy()
    
    return g.sort_values("area_ha", ascending=False).head(10).reset_index(drop=True)


def build_union_buffer(g: gpd.GeoDataFrame) -> gpd.GeoSeries | None:
    """Union all course buffers."""
    if g.empty:
        return None
    buf = g.geometry.buffer(BUFFER_M).unary_union
    return buf if buf is not None else None


def weighted_stats(label: str, buffer_geom, tracts: gpd.GeoDataFrame, course_count: int) -> dict:
    """Compute population-weighted statistics."""
    if buffer_geom is None:
        df = tracts.copy()
        df["inter_area_m2"] = df["tract_area_m2"] if "tract_area_m2" in df.columns else df.geometry.area
        df["weight_pop"] = df.get("pop_2021", 0).fillna(0)
    else:
        mask = gpd.GeoDataFrame(geometry=[buffer_geom], crs=tracts.crs)
        inter = gpd.overlay(tracts, mask, how="intersection")
        if inter.empty:
            return {
                "Group": label,
                "Courses": course_count,
                "Population": 0,
                "Median HH Income": np.nan,
                "Renters %": np.nan,
                "Age 65+ %": np.nan,
                "Pop Density (/km²)": np.nan,
                "Area (km²)": 0,
            }
        inter["inter_area_m2"] = inter.geometry.area
        inter["weight_pop"] = inter.get("pop_2021", 0).fillna(0) * (inter["inter_area_m2"] / inter.get("tract_area_m2", inter["inter_area_m2"]))
        df = inter
    
    pop_weight = df["weight_pop"].sum()
    area_km2 = df["inter_area_m2"].sum() / 1e6
    
    def wavg(series):
        s = pd.to_numeric(series, errors="coerce")
        if pop_weight <= 0 or s.isna().all():
            return np.nan
        return np.average(s.fillna(0), weights=df["weight_pop"])
    
    income = wavg(df.get("median_hh_income_2020"))
    renters = wavg(df.get("pct_renters"))
    seniors = wavg(df.get("pct_65plus"))
    density = pop_weight / area_km2 if area_km2 > 0 else np.nan
    
    return {
        "Group": label,
        "Courses": course_count,
        "Population": pop_weight,
        "Median HH Income": income,
        "Renters %": renters,
        "Age 65+ %": seniors,
        "Pop Density (/km²)": density,
        "Area (km²)": area_km2,
    }


def format_table(df: pd.DataFrame) -> plt.Figure:
    """Format results as styled table."""
    fig, ax = plt.subplots(figsize=(9, 6.5))
    ax.axis("off")
    ax.set_title("Census averages near Vancouver golf courses (800 m buffers)", 
                 fontsize=14, fontweight="bold", pad=16)
    
    display_df = df.copy()
    display_df["Median HH Income"] = display_df["Median HH Income"].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "—")
    display_df["Renters %"] = display_df["Renters %"].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "—")
    display_df["Age 65+ %"] = display_df["Age 65+ %"].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "—")
    display_df["Pop Density (/km²)"] = display_df["Pop Density (/km²)"].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "—")
    display_df["Area (km²)"] = display_df["Area (km²)"].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else "—")
    
    display_df = display_df.set_index("Group").T
    display_df = display_df.reset_index()
    display_df = display_df.rename(columns={"index": "Metric"})
    display_df = display_df[display_df["Metric"] != "Population"]
    
    table = ax.table(
        cellText=display_df.values,
        colLabels=display_df.columns,
        cellLoc="center",
        loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    
    for i in range(1, len(display_df) + 1):
        for j in range(len(display_df.columns)):
            table[(i, j)].set_facecolor("#f0f0f0" if i % 2 == 1 else "#ffffff")
            table[(i, j)].set_edgecolor("#dddddd")
            table[(i, j)].set_linewidth(1.0)
    
    for j in range(len(display_df.columns)):
        table[(0, j)].set_facecolor("#343a40")
        table[(0, j)].set_text_props(color="white", fontweight="bold", fontsize=12)
        table[(0, j)].set_edgecolor("#343a40")
        table[(0, j)].set_height(0.08)
    
    table.scale(1, 2.0)
    ax.text(0.01, -0.08, "Population-weighted averages of intersecting tracts; 800 m buffers; OSM golf courses (>=1 ha).", 
            fontsize=9, color="#555555", transform=ax.transAxes)
    
    fig.patch.set_facecolor("white")
    return fig


# ============================================================================
# MAIN
# ============================================================================


def main() -> None:
    """Execute census summary analysis."""
    print("Loading census tracts and metrics...")
    tracts = load_tracts_with_metrics()
    
    print("Fetching golf courses from OpenStreetMap...")
    golf = fetch_golf_courses()
    print(f"Courses found (after filtering): {len(golf)}")
    
    if golf.empty:
        raise SystemExit("No golf courses found; aborting.")
    
    results = []
    
    for access in ACCESS_TYPES:
        subset = golf[golf["access_type"] == access]
        buf = build_union_buffer(subset)
        results.append(weighted_stats(access, buf, tracts, course_count=len(subset)))
    
    results.append(weighted_stats("City Average", None, tracts, course_count=len(golf)))
    
    summary_df = pd.DataFrame(results)
    fig = format_table(summary_df)
    out_path = OUTPUT_DIR / "course_census_summary.png"
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    
    print(f"Saved summary table: {out_path}")


if __name__ == "__main__":
    main()
