"""
Collect city demographic and socioeconomic indicators using pytidycensus (ACS) for US
and stats_can for Canadian cities.

Requirements:
- For US: Environment variable `CENSUS_API_KEY` must be set
- Python packages: pytidycensus, pandas, us (US), stats_can (Canada)

Usage:
    python collect_city_demographics.py

By default this will collect demographics for all cities listed in
`collect_city_amenities.US_CITIES` and `collect_city_amenities.CANADIAN_CITIES`
and save/update `data/city_demographics.json` keyed by city slug.
"""

import os
import json
from pathlib import Path
from typing import Dict, Any, List, Tuple

import pandas as pd

try:
    import pytidycensus as ptc
except ImportError:
    ptc = None
    print("Warning: pytidycensus not available. US demographics will be skipped.")

try:
    import us
except ImportError:
    us = None
    print("Warning: us package not available. US demographics will be skipped.")

try:
    import stats_can
except ImportError:
    stats_can = None
    print("Warning: stats_can not available. Canadian demographics will be skipped.")

try:
    import pycancensus as pc
except ImportError:
    pc = None
    print("Warning: pycancensus not available. Canadian demographics via CensusMapper will be skipped.")


# Curated ACS5 variable map: key -> (label, variable id)
ACS5_VARS: Dict[str, str] = {
    # Population totals
    "population_total": "B01003_001E",
    # Age
    "median_age": "B01002_001E",
    # Housing
    "housing_units": "B25001_001E",
    "median_home_value": "B25077_001E",
    "median_gross_rent": "B25064_001E",
    "owner_occupied_units": "B25003_002E",
    "renter_occupied_units": "B25003_003E",
    # Income & poverty
    "median_household_income": "B19013_001E",
    "per_capita_income": "B19301_001E",
    "poverty_rate_total": "B17001_002E",  # numerators; will compute rate if total is present
    "poverty_population_total": "B17001_001E",
    # Employment
    "civilian_labor_force": "B23025_003E",
    "employed_total": "B23025_004E",
    "unemployed_total": "B23025_005E",
    # Education
    "bachelors_or_higher_25_plus": "B06009_005E",
    "graduate_or_professional_25_plus": "B06009_006E",
    # Race
    "white_alone": "B02001_002E",
    "black_or_african_american_alone": "B02001_003E",
    "asian_alone": "B02001_005E",
    "two_or_more_races": "B02001_008E",
    # Hispanic origin
    "hispanic_or_latino_any_race": "B03003_003E",
    "not_hispanic_or_latino": "B03003_002E",
    # Immigration / nativity
    "foreign_born": "B05002_013E",
    "native_born": "B05002_002E",
    # Housing costs burden
    "renter_gross_rent_35_plus_income": "B25070_010E",
}


def _city_slug(city: str, state: str) -> str:
    return f"{city.strip().replace(' ', '_').lower()}_{state.strip().lower()}"


def _resolve_state_abbr(state_name_or_abbr: str) -> str | None:
    """Resolve state input (full name or abbreviation) to a 2-letter abbreviation."""
    if not state_name_or_abbr:
        return None

    # Common special cases
    specials = {
        "District of Columbia": "DC",
        "Washington, D.C.": "DC",
        "Washington DC": "DC",
    }
    if state_name_or_abbr in specials:
        return specials[state_name_or_abbr]

    state_obj = us.states.lookup(state_name_or_abbr)
    return state_obj.abbr if state_obj else None


def _chunk_list(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def fetch_place_acs(city: str, state_abbr: str, year: int = 2022, survey: str = "ACS5", api_key: str | None = None, chunk_size: int = 8) -> pd.DataFrame:
    """
    Fetch ACS variables for a city/place using pytidycensus.

    Uses chunked variable requests to reduce partial failures where only one table is returned.
    """
    api_key = api_key or os.environ.get("CENSUS_API_KEY")
    if not api_key:
        raise RuntimeError("CENSUS_API_KEY environment variable is missing.")

    state_abbr_clean = str(state_abbr).strip()
    if state_abbr_clean.upper() == 'DC':
        state_obj = us.states.DC
    else:
        state_obj = us.states.lookup(state_abbr_clean)
    if state_obj is None:
        raise ValueError(f"Invalid state abbreviation: {state_abbr}")

    vars_list: List[str] = list(ACS5_VARS.values())
    survey_norm = survey.lower() if isinstance(survey, str) else 'acs5'

    merged_df: pd.DataFrame | None = None
    chunks = _chunk_list(vars_list, chunk_size)

    for chunk in chunks:
        part = ptc.get_acs(
            geography='place',
            variables=chunk,
            year=year,
            survey=survey_norm,
            state=state_obj.fips,
            output='wide',
            geometry=False,
            api_key=api_key,
        )

        if merged_df is None:
            merged_df = part
            continue

        # Merge on common identifiers
        keys = [k for k in ['GEOID', 'state', 'place', 'NAME', 'name'] if k in merged_df.columns and k in part.columns]
        if not keys:
            raise RuntimeError("Could not find common keys to merge ACS chunks.")
        merged_df = merged_df.merge(part, on=keys, how='left', suffixes=('', '_dup'))

        # Drop duplicate columns that came from suffixing
        dup_cols = [col for col in merged_df.columns if col.endswith('_dup')]
        if dup_cols:
            merged_df = merged_df.drop(columns=dup_cols)

    df = merged_df if merged_df is not None else pd.DataFrame()

    # Filter rows where place name matches the city (case-insensitive contains)
    name_col = None
    for cand in ["NAME", "name"]:
        if cand in df.columns:
            name_col = cand
            break
    if name_col is None:
        for cand in ["place_name", "geography_name"]:
            if cand in df.columns:
                name_col = cand
                break
    if name_col is None:
        raise RuntimeError("Could not find place name column in ACS response.")

    mask = df[name_col].str.contains(city, case=False, na=False)
    filtered = df.loc[mask].copy()
    if filtered.empty:
        raise RuntimeError(f"No ACS place rows matched city '{city}' in {state_abbr}.")

    return filtered


def aggregate_place_row(row_group: pd.DataFrame) -> Dict[str, Any]:
    """
    Aggregate multiple place rows (if a city spans multiple places) into a single record.
    For most indicators we sum counts and keep medians as population-weighted where possible.
    """
    out: Dict[str, Any] = {}

    # Map ACS variables back to our keys
    # The tidy output uses the raw variable codes as column names
    for key, var in ACS5_VARS.items():
        if var in row_group.columns:
            series = pd.to_numeric(row_group[var], errors="coerce")
            if series.notna().any():
                # Heuristic: treat median_* as population-weighted by total population
                if key.startswith("median_"):
                    pop = pd.to_numeric(row_group.get("B01003_001E", pd.Series([None]*len(row_group))), errors="coerce")
                    weight = pop.fillna(0)
                    val = (series.fillna(0) * weight).sum() / max(weight.sum(), 1)
                else:
                    # Sum counts; for rates like poverty_rate_total we'll compute after
                    val = series.sum()
                out[key] = float(val)

    # Compute derived rates when possible
    # Unemployment rate = unemployed / labor force
    lf = out.get("civilian_labor_force", None)
    unemp = out.get("unemployed_total", None)
    if lf and lf > 0 and unemp is not None:
        out["unemployment_rate"] = float(unemp) / float(lf)

    # Poverty rate = poverty numerators / poverty universe
    pov_num = out.get("poverty_rate_total", None)
    pov_den = out.get("poverty_population_total", None)
    if pov_num and pov_den and pov_den > 0:
        out["poverty_rate"] = float(pov_num) / float(pov_den)

    # Owner vs renter share
    owners = out.get("owner_occupied_units", None)
    renters = out.get("renter_occupied_units", None)
    occ_total = None
    if owners is not None or renters is not None:
        occ_total = (owners or 0) + (renters or 0)
    if occ_total and occ_total > 0:
        out["owner_share"] = float(owners or 0) / occ_total
        out["renter_share"] = float(renters or 0) / occ_total

    # Race shares
    pop_total = out.get("population_total", None)
    for race_key in [
        "white_alone",
        "black_or_african_american_alone",
        "asian_alone",
        "two_or_more_races",
        "hispanic_or_latino_any_race",
        "not_hispanic_or_latino",
    ]:
        val = out.get(race_key, None)
        if pop_total and val is not None and pop_total > 0:
            out[f"{race_key}_share"] = float(val) / float(pop_total)

    return out


def collect_city_demographics(city: str, state_abbr: str, year: int = 2022, api_key: str | None = None) -> Dict[str, Any]:
    """Collect and aggregate ACS5 indicators into a dictionary keyed by city."""
    df = fetch_place_acs(city, state_abbr, year=year, api_key=api_key)

    # Aggregate to a single row if multiple places match
    agg = aggregate_place_row(df)

    return agg


def save_city_demographics_json(city: str, state_abbr: str, out_path: str, year: int = 2022, api_key: str | None = None) -> str:
    """Save city demographics as JSON: {city_slug: {...}}."""
    city_key = _city_slug(city, state_abbr)
    data = collect_city_demographics(city, state_abbr, year=year, api_key=api_key)

    # Load existing JSON if present and update
    out_file = Path(out_path)
    if out_file.exists():
        try:
            with open(out_file, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            existing = {}
    else:
        existing = {}

    existing[city_key] = data

    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2)

    return str(out_file)


def batch_save_us_amenity_cities(out_path: str, year: int = 2022, api_key: str | None = None, skip_existing: bool = True) -> Tuple[int, int]:
    """Collect demographics for the US amenity cities list and persist to JSON.

    Returns a tuple of (fetched_count, skipped_count).
    """
    try:
        from collect_city_amenities import US_CITIES
    except Exception as exc:
        raise ImportError("Unable to import US_CITIES from collect_city_amenities") from exc

    out_file = Path(out_path)
    if out_file.exists():
        try:
            with open(out_file, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            existing = {}
    else:
        existing = {}

    fetched = 0
    skipped = 0

    for city, state_name in US_CITIES:
        state_abbr = _resolve_state_abbr(state_name)
        if not state_abbr:
            print(f"Skipping {city}, could not resolve state '{state_name}' to abbreviation.")
            skipped += 1
            continue

        city_key = _city_slug(city, state_abbr)
        if skip_existing and city_key in existing:
            skipped += 1
            continue

        try:
            data = collect_city_demographics(city, state_abbr, year=year, api_key=api_key)
            existing[city_key] = data
            fetched += 1
            print(f"Fetched {city} ({state_abbr})")
        except Exception as exc:
            print(f"Failed {city} ({state_abbr}): {exc}")
            skipped += 1

    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2)

    return fetched, skipped


# ============================================================================
# Canadian Census Data Collection (using stats_can)
# ============================================================================

# Map Canadian census variable names to comparable fields with US
# Based on Census Profile 2021
STATSCAN_PROFILE_VARS: Dict[str, str] = {
    # Population
    "population_total": "Population, 2021",
    "population_density": "Population density per square kilometre",
    # Age
    "median_age": "Median age of the population",
    # Housing
    "total_private_dwellings": "Total private dwellings",
    "occupied_private_dwellings": "Private dwellings occupied by usual residents",
    "median_home_value": "Average value of dwellings ($)",
    "median_monthly_shelter_cost_owners": "Median monthly shelter costs for owned dwellings ($)",
    "median_monthly_shelter_cost_renters": "Median monthly shelter costs for rented dwellings ($)",
    "owner_households": "Owner",
    "renter_households": "Renter",
    # Income
    "median_household_income": "Median total income of household in 2020 ($)",
    "median_individual_income": "Median total income in 2020 among recipients ($)",
    # Employment
    "employment_rate": "Employment rate",
    "unemployment_rate": "Unemployment rate",
    "participation_rate": "Participation rate",
    # Education (25-64 age group typically)
    "no_certificate": "No certificate, diploma or degree",
    "high_school": "Secondary (high) school diploma or equivalency certificate",
    "bachelors_degree": "Bachelor's degree",
    "university_above_bachelors": "University certificate, diploma or degree above bachelor level",
    # Immigration
    "immigrants": "Immigrants",
    "non_immigrants": "Non-immigrants",
    # Visible minority
    "total_visible_minority": "Total visible minority population",
    "not_visible_minority": "Not a visible minority",
}


def _province_slug(province: str) -> str:
    """Convert province name to slug for key matching."""
    mapping = {
        "Ontario": "on",
        "Quebec": "qc",
        "British Columbia": "bc",
        "Alberta": "ab",
        "Manitoba": "mb",
        "Saskatchewan": "sk",
        "Nova Scotia": "ns",
        "New Brunswick": "nb",
        "Newfoundland and Labrador": "nl",
        "Prince Edward Island": "pe",
    }
    return mapping.get(province, province.lower().replace(" ", "_"))


def fetch_canadian_census(city: str, province: str, year: int = 2021, api_key: str | None = None) -> Dict[str, Any]:
    """
    Fetch Canadian census data for a city using pycancensus (CensusMapper API).
    
    This function ONLY returns data from the official Statistics Canada API via pycancensus.
    No fallback to guessed data - if the API fails, it raises an exception.
    
    Args:
        city: City name (e.g., "Toronto", "Vancouver")
        province: Province name (e.g., "Ontario", "British Columbia")
        year: Census year (default 2021)
        api_key: CensusMapper API key (or via CANCENSUS_API_KEY env var)
    
    Returns:
        Dictionary of demographic indicators from Statistics Canada API
        
    Raises:
        RuntimeError: If data cannot be fetched from pycancensus API
    """
    out: Dict[str, Any] = {}
    
    if not pc:
        raise ImportError("pycancensus is required. Install via 'pip install pycancensus'")
    
    # Set API key if provided
    if api_key:
        os.environ["CANCENSUS_API_KEY"] = api_key
    
    # Census Metropolitan Area (CMA) names mapping
    cma_mapping = {
        "Toronto": "Toronto",
        "Vancouver": "Vancouver",
        "Montreal": "Montréal",
        "Montreal (English)": "Montreal",
        "Calgary": "Calgary",
        "Edmonton": "Edmonton",
        "Ottawa": "Ottawa-Gatineau",
        "Winnipeg": "Winnipeg",
        "Quebec City": "Québec",
        "Hamilton": "Hamilton",
        "London": "London",
        "Kitchener": "Kitchener-Cambridge-Waterloo",
        "St. Catharines": "St. Catharines-Niagara",
    }
    
    cma_name = cma_mapping.get(city, city)
    dataset = "CA21"  # 2021 Census
    
    try:
        # Search for the city region
        regions_df = pc.search_census_regions(cma_name, dataset=dataset)
        
        if regions_df.empty:
            raise RuntimeError(f"Could not find region '{cma_name}' in pycancensus")
        
        # Get the first matching region (usually the CMA)
        region_id = regions_df.iloc[0]["region"]
        print(f"  Found region {region_id} for {city}")
        
        # Comprehensive list of 2021 Census vectors
        # These are REAL official Statistics Canada vectors
        census_vectors = [
            "v_CA21_1",      # Population, 2021
            "v_CA21_389",    # Median age (CORRECTED - was v_CA21_22)
            "v_CA21_4237",   # Total private households by tenure
            "v_CA21_4238",   # Owner households
            "v_CA21_4239",   # Renter households
            "v_CA21_575",    # Employment income recipients (proxy for employed)
            "v_CA21_5799",   # High school diploma or equivalency
            "v_CA21_4404",   # Total - Immigrant status
            "v_CA21_4405",   # Not an immigrant
            "v_CA21_4406",   # Immigrant
            "v_CA21_4872",   # Total visible minority
            "v_CA21_4873",   # White (not visible minority)
            "v_CA21_4874",   # South Asian
            "v_CA21_4875",   # Chinese
            "v_CA21_4876",   # Black
        ]
        
        # Fetch data for this region and vectors
        print(f"  Fetching {len(census_vectors)} official Statistics Canada vectors for {city}...")
        
        # Suppress pycancensus output (contains emojis that cause encoding errors on Windows)
        import contextlib
        from io import StringIO
        
        f = StringIO()
        with contextlib.redirect_stdout(f):
            census_df = pc.get_census(
                dataset=dataset,
                regions={"CMA": [region_id]},
                vectors=census_vectors,
                use_cache=True
            )
        
        print(f"  Retrieved {len(census_df.columns)} columns from Statistics Canada API")
        
        # Map vector IDs to field names (using CORRECTED vector IDs)
        vector_mapping = {
            "v_CA21_1": "population_total",
            "v_CA21_389": "median_age",
            "v_CA21_4237": "total_households",
            "v_CA21_4238": "owner_occupied_units",
            "v_CA21_4239": "renter_occupied_units",
            "v_CA21_575": "employed_total",
            "v_CA21_5799": "high_school",
            "v_CA21_4404": "total_immigrant_status",
            "v_CA21_4405": "native_born",
            "v_CA21_4406": "foreign_born",
            "v_CA21_4872": "total_visible_minority",
            "v_CA21_4873": "white_alone",
            "v_CA21_4874": "south_asian",
            "v_CA21_4875": "chinese",
            "v_CA21_4876": "black_or_african_american_alone",
        }
        
        # Parse the dataframe and extract values
        # Column names come with format: "v_CA21_1: Population, 2021"
        for col in census_df.columns:
            if col not in ["GeoUID", "Region Name", "Type", "PR_UID", "Area (sq km)", "Population", "Dwellings", "Households", "rpid", "rgid", "ruid", "rguid"]:
                # Extract vector ID from column name (e.g., "v_CA21_1: ..." -> "v_CA21_1")
                vec_id = col.split(":")[0].strip() if ":" in col else col
                
                if vec_id in vector_mapping:
                    val = census_df[col].iloc[0]
                    if pd.notna(val):
                        try:
                            numeric_val = float(val)
                            field_name = vector_mapping[vec_id]
                            out[field_name] = numeric_val
                        except (ValueError, TypeError):
                            pass
        
        if not out:
            raise RuntimeError(f"No data could be extracted from API response for {city}")
        
        # Calculate derived fields from official data
        # Unemployment rate
        if "unemployed_total" in out and "civilian_labor_force" in out and out["civilian_labor_force"] > 0:
            out["unemployment_rate"] = out["unemployed_total"] / out["civilian_labor_force"]
        
        # Owner/renter shares
        if "owner_occupied_units" in out and "renter_occupied_units" in out:
            total = out["owner_occupied_units"] + out["renter_occupied_units"]
            if total > 0:
                out["owner_share"] = out["owner_occupied_units"] / total
                out["renter_share"] = out["renter_occupied_units"] / total
        
        # Visible minority share
        if "total_visible_minority" in out and "population_total" in out and out["population_total"] > 0:
            out["total_visible_minority_share"] = out["total_visible_minority"] / out["population_total"]
        
        # Race/ethnicity shares
        if "population_total" in out and out["population_total"] > 0:
            for race_key in ["white_alone", "black_or_african_american_alone", "south_asian", "chinese"]:
                if race_key in out:
                    out[f"{race_key}_share"] = out[race_key] / out["population_total"]
        
        # Foreign born share
        if "foreign_born" in out and "population_total" in out and out["population_total"] > 0:
            out["foreign_born_share"] = out["foreign_born"] / out["population_total"]
        
        print(f"  Successfully retrieved {len(out)} fields from Statistics Canada")
        out["data_source"] = f"Statistics Canada 2021 Census via CensusMapper pycancensus"
        out["city"] = city
        out["province"] = province
        out["retrieved_at"] = pd.Timestamp.now().isoformat()
        
        return out
        
    except Exception as e:
        raise RuntimeError(f"Failed to fetch Canadian census data for {city}, {province} from Statistics Canada API: {e}")


def collect_canadian_city_demographics(city: str, province: str, year: int = 2021, api_key: str | None = None) -> Dict[str, Any]:
    """Collect Canadian census data for a city."""
    return fetch_canadian_census(city, province, year=year, api_key=api_key)


def batch_save_canadian_amenity_cities(out_path: str, year: int = 2021, api_key: str | None = None, skip_existing: bool = True) -> Tuple[int, int]:
    """Collect demographics for Canadian amenity cities and persist to JSON.
    
    Returns a tuple of (fetched_count, skipped_count).
    """
    if pc is None and stats_can is None:
        print("pycancensus and stats_can not available, using fallback census data")
    
    try:
        from collect_city_amenities import CANADIAN_CITIES
    except Exception as exc:
        raise ImportError("Unable to import CANADIAN_CITIES from collect_city_amenities") from exc
    
    out_file = Path(out_path)
    if out_file.exists():
        try:
            with open(out_file, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            existing = {}
    else:
        existing = {}
    
    fetched = 0
    skipped = 0
    
    for city, province in CANADIAN_CITIES:
        province_abbr = _province_slug(province)
        city_key = _city_slug(city, province_abbr)
        
        if skip_existing and city_key in existing:
            skipped += 1
            continue
        
        try:
            data = collect_canadian_city_demographics(city, province, year=year, api_key=api_key)
            existing[city_key] = data
            fetched += 1
            print(f"Fetched {city} ({province})")
        except Exception as exc:
            print(f"Failed {city} ({province}): {exc}")
            skipped += 1
    
    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2)
    
    return fetched, skipped


def main():
    # Collect demographics for both US and Canadian cities
    api_key_us = os.environ.get("CENSUS_API_KEY")
    api_key_ca = os.environ.get("CANCENSUS_API_KEY") or "CensusMapper_d918ab7e2b0cb08ac7b24a3990a6cb93"
    out_path = Path("data") / "city_demographics.json"

    total_fetched = 0
    total_skipped = 0

    # US cities
    if ptc and us:
        print("Collecting US city demographics...")
        fetched, skipped = batch_save_us_amenity_cities(
            out_path=out_path,
            year=2022,
            api_key=api_key_us,
            skip_existing=False,
        )
        total_fetched += fetched
        total_skipped += skipped
        print(f"US: Fetched {fetched}, skipped {skipped}")
    else:
        print("Skipping US cities (pytidycensus or us package not available)")

    # Canadian cities
    if pc:
        print("\nCollecting Canadian city demographics via pycancensus...")
        fetched, skipped = batch_save_canadian_amenity_cities(
            out_path=out_path,
            year=2021,
            api_key=api_key_ca,
            skip_existing=False,
        )
        total_fetched += fetched
        total_skipped += skipped
        print(f"Canadian: Fetched {fetched}, skipped {skipped}")
    else:
        print("Skipping Canadian cities via pycancensus (not available)")

    print(f"\nBatch complete. Total fetched {total_fetched}, skipped {total_skipped}. Saved to {out_path}")


if __name__ == "__main__":
    main()
