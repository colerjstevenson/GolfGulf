# Golf Analysis Package

This folder contains the code and outputs for golf course analysis including parks comparison, census summary statistics, and interactive maps.

## Folder Structure

```
golf_analysis_package/
├── code/
│   ├── paper_generate_all_maps.py                 # Master pipeline for paper maps
│   ├── paper_build_analytic_tracts_vancouver.py   # Build analytic tracts
│   ├── paper_metrics_vancouver.py                 # Compute census metrics
│   ├── paper_build_course_exposure_vancouver.py   # Course exposure metrics
│   ├── paper_make_static_maps_vancouver.py        # Generate static quintile maps
│   ├── paper_helpers.py                           # Helper functions
│   ├── scrape_and_compare_golf_parks.py           # Parks vs golf comparison
│   └── course_census_summary.py                   # Census statistics summary
└── outputs/
    ├── paper_maps/                                # Paper-quality quintile maps
    │   ├── map_income_quintiles.png/.pdf
    │   ├── map_renters_quintiles.png/.pdf
    │   ├── map_seniors_quintiles.png/.pdf
    │   ├── map_population_quintiles.png/.pdf
    │   └── map_density_quintiles.png/.pdf
    ├── parks_comparison/                          # Parks vs golf analysis outputs
    │   ├── golf_vs_parks_counts.png
    │   ├── golf_vs_parks_areas.png
    │   ├── golf_vs_parks_with_stanley.png
    │   ├── golf_vs_parks_without_stanley.png
    │   └── golf_vs_parks_summary.csv
    └── census_summary/                            # Census summary table
        └── course_census_summary.png
```

## Key Outputs

### Paper Maps (Quintile Analysis)
- **Folder**: `outputs/paper_maps/`
- Publication-ready maps showing census metrics in quintiles with color-coded golf courses
- Includes PNG and PDF versions for income, renters, seniors, population, and density
- Golf courses color-coded by access type: Red (Private), Purple (Municipal), Blue (Public)

### Parks Comparison
- **Folder**: `outputs/parks_comparison/`
- **Files**: `golf_vs_parks_counts.png`, `golf_vs_parks_areas.png`, `golf_vs_parks_with/without_stanley.png`
- Analysis comparing golf courses to parks/green spaces
- Summary statistics in `golf_vs_parks_summary.csv`

### Census Summary Table
- **File**: `outputs/census_summary/course_census_summary.png`
- Population-weighted census averages showing:
  - Median Household Income
  - Percentage Renters
  - Percentage Age 65+
  - Population Density
  - Area (km²)
- Comparison across Private, Municipal, Public golf courses with city average

### Statistical Tables
- **Folder**: `outputs/census_analysis/`
- CSV exports of correlation analyses, summary statistics, and detailed metrics

## Running the Code

### Course Census Summary
```bash
python code/course_census_summary.py
```
Generates a styled PNG table with census statistics by golf course access type.

### Parks Comparison
```bash
python code/scrape_and_compare_golf_parks.py
```
Analyzes and compares golf courses to parks/green spaces in the city.

### Generate Paper Maps (Recommended)
```bash
python code/paper_generate_all_maps.py
```
Master script that orchestrates the complete pipeline to generate all quintile maps with color-coded golf courses. Runs all prerequisite steps automatically.

### Individual Paper Pipeline Steps
```bash
# 1. Build analytic tracts
python code/paper_build_analytic_tracts_vancouver.py

# 2. Compute census metrics
python code/paper_metrics_vancouver.py

# 3. Calculate course exposure
python code/paper_build_course_exposure_vancouver.py

# 4. Generate static maps
python code/paper_make_static_maps_vancouver.py
```

## Data Requirements

The scripts require:
- Census tract data (GeoJSON format)
- Census metrics CSV (income, demographics, density)
- Golf course data from OpenStreetMap
- Parks/amenities data

Ensure these data files are available before running the scripts.

## Notes

- 800m buffer zones used for all proximity-based analyses
- Data is population-weighted where applicable
- Maps use Folium for interactive visualization
- All outputs are generated in high resolution (300 DPI for images)
