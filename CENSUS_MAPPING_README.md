# Census Tract Choropleth Mapper

Create interactive choropleth maps of Canadian census tract data with automatic metric discovery.

## Quick Start

### 1. Pre-cache Cities (Recommended)

The nationwide census CSV is very large (~GB). Pre-cache cities you want to work with:

```powershell
python build_city_caches.py
```

This will process all major Canadian cities and create filtered cache files. You can:
- Run it overnight (takes hours for all cities)
- Interrupt and resume anytime (skips already cached cities)
- Add/remove cities in the script before running

### 2. Generate Maps

Once cached, mapping is instant:

```powershell
python ct_choropleth.py Vancouver BC
python ct_choropleth.py Toronto Ontario
python ct_choropleth.py Montreal Quebec
```

Maps are saved to the `maps/` folder with all census characteristics in an interactive dropdown.

## Files

- `ct_choropleth.py` - Main script to generate maps (uses cache if available)
- `build_city_caches.py` - Pre-cache multiple cities at once
- `city_cache.py` - Caching logic (imported by both scripts)

## Data Requirements

Place these files in `data/censusShape/`:
- Census tract shapefile (e.g., `lct_000b2021a_e.shp` + supporting files)
- Census Profile CSV (`98-401-X2021007_English_CSV_data.csv`)
- Geo mapping CSV (`98-401-X2021007_Geo_starting_row.CSV`)

Download from Statistics Canada: https://www12.statcan.gc.ca/census-recensement/2021/dp-pd/prof/index.cfm

## How It Works

1. **Geocoding**: Uses OpenStreetMap to find city boundaries
2. **Clipping**: Filters census tracts to the city polygon  
3. **Caching**: Extracts only city rows from the nationwide CSV (done once)
4. **Mapping**: Loads all characteristics and builds interactive Folium map
5. **Output**: HTML file with dropdown to switch between metrics

## Cached Cities

After running `build_city_caches.py`, you'll have cached data in:
```
data/censusShape/
  vancouver/
  toronto/
  montreal/
  ...
```

Each cache contains only the rows for that city's census tracts, making subsequent map generation very fast.
