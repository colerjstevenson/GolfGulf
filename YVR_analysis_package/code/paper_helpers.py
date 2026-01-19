# paper_helpers.py
from __future__ import annotations

from typing import Iterable
import geopandas as gpd
import pandas as pd

EXCLUDE_COURSES_DEFAULT = {
    "Stanley Park Pitch & Putt",
}

def _clean_course_name(s: pd.Series) -> pd.Series:
    return (
        s.astype("string")
         .str.strip()
         .str.replace(r"\s+", " ", regex=True)
    )

def load_golf_courses(path, exclude_courses: Iterable[str] = EXCLUDE_COURSES_DEFAULT) -> gpd.GeoDataFrame:
    golf = gpd.read_file(path)

    # Standardize name column -> course_name
    if "course_name" not in golf.columns and "name" in golf.columns:
        golf = golf.rename(columns={"name": "course_name"})

    if "course_name" not in golf.columns:
        raise RuntimeError(f"Golf file missing course name column. Columns: {list(golf.columns)}")

    golf["course_name"] = _clean_course_name(golf["course_name"])

    exclude_courses = set(exclude_courses) if exclude_courses else set()
    if exclude_courses:
        golf = golf[~golf["course_name"].isin(exclude_courses)].copy()

    return golf.reset_index(drop=True)
