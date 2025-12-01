import requests
import os
import zipfile
import shutil
from pathlib import Path
from tqdm import tqdm

URL = "https://github.com/colerjstevenson/GolfGulf/releases/download/data/golf_data.zip"
ZIP_PATH = Path("data/golf_data.zip")
EXTRACT_DIR = Path("data")

def confirm_overwrite() -> bool:
    """Ask user for confirmation to overwrite existing data."""
    while True:
        response = input("Data folder already exists. Overwrite existing files? (y/n): ").strip().lower()
        if response in ('y', 'yes'):
            return True
        elif response in ('n', 'no'):
            return False
        else:
            print("Please enter 'y' or 'n'")

def download(url: str, path: Path) -> None:
    """Download file from URL to path with progress bar."""
    path.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(url, stream=True)
    response.raise_for_status()
    total = int(response.headers.get("content-length", 0))
    with open(path, "wb") as f, tqdm(total=total, unit="B", unit_scale=True, desc="Downloading") as bar:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
                bar.update(len(chunk))

def extract_zip(zip_path: Path, extract_to: Path, remove_zip: bool = True) -> Path:
    """Extract zip and flatten nested golf_data folder if present. Returns temp extraction path."""
    # Extract to a temporary location first
    temp_extract = extract_to / '_temp_extract'
    temp_extract.mkdir(parents=True, exist_ok=True)
    
    with zipfile.ZipFile(zip_path, 'r') as zf:
        zf.extractall(temp_extract)

    # If the zip contained a top-level golf_data/ folder, use that, otherwise use temp_extract
    nested_dir = temp_extract / 'golf_data'
    if nested_dir.is_dir():
        extraction_root = nested_dir
    else:
        extraction_root = temp_extract

    if remove_zip:
        try:
            zip_path.unlink()
        except OSError:
            pass
    
    return extraction_root

def organize_data_files(source_dir: Path, overwrite: bool = True) -> None:
    """
    Organize downloaded files into expected locations:
    - data/canada/Fully_Matched_Golf_Courses.csv (used by interactive_map_builder, map_generator)
    - data/censusShape/ (contains .shp files, used by census_cacher, interactive_map_builder)
    - Other regional data files in data/usa/, data/mexico/ if present
    
    Only overwrites files that came from the download. Existing files not in the download are preserved.
    """
    print("Organizing data files into expected structure...")
    
    data_dir = Path("data")
    
    # Expected structure:
    # - data/canada/ should contain Fully_Matched_Golf_Courses.csv
    # - data/censusShape/ should contain .shp and related shapefile files
    # - data/usa/, data/mexico/ for other regions
    
    canada_dir = data_dir / "canada"
    census_dir = data_dir / "censusShape"
    
    # Ensure directories exist
    canada_dir.mkdir(exist_ok=True)
    census_dir.mkdir(exist_ok=True)
    
    # Move golf course CSV files to regional folders
    for csv_file in source_dir.glob("*Fully_Matched_Golf_Courses*.csv"):
        # Check if it should go to canada, usa, or mexico based on filename
        if "canada" in csv_file.name.lower() or csv_file.name == "Fully_Matched_Golf_Courses.csv":
            target = canada_dir / "Fully_Matched_Golf_Courses.csv"
        elif "usa" in csv_file.name.lower():
            (data_dir / "usa").mkdir(exist_ok=True)
            target = data_dir / "usa" / "Fully_Matched_Golf_Courses.csv"
        elif "mexico" in csv_file.name.lower():
            (data_dir / "mexico").mkdir(exist_ok=True)
            target = data_dir / "mexico" / "Fully_Matched_Golf_Courses.csv"
        else:
            # Default to canada
            target = canada_dir / "Fully_Matched_Golf_Courses.csv"
        
        if target.exists() and not overwrite:
            print(f"  Skipped {csv_file.name} (file exists)")
        else:
            shutil.copy2(str(csv_file), str(target))
            print(f"  {'Overwrote' if target.exists() else 'Moved'} {csv_file.name} → {target.parent.name}/")
    
    # Move shapefiles to censusShape/
    shapefile_extensions = ['.shp', '.shx', '.dbf', '.prj', '.cpg', '.sbn', '.sbx']
    moved_shp = False
    for ext in shapefile_extensions:
        for shp_file in source_dir.glob(f"*{ext}"):
            target = census_dir / shp_file.name
            if target.exists() and not overwrite:
                if ext == '.shp':  # Only print for .shp to avoid spam
                    print(f"  Skipped {shp_file.name} (file exists)")
            else:
                shutil.copy2(str(shp_file), str(target))
                if ext == '.shp' and not moved_shp:  # Only print once for shapefiles
                    print(f"  {'Overwrote' if target.exists() else 'Moved'} shapefiles → censusShape/")
                    moved_shp = True
    
    # Move any other regional CSV files (combined.csv, etc.)
    for region in ["canada", "usa", "mexico"]:
        for csv_file in source_dir.glob(f"*{region}*.csv"):
            if csv_file.name != "Fully_Matched_Golf_Courses.csv":
                region_dir = data_dir / region
                region_dir.mkdir(exist_ok=True)
                target = region_dir / csv_file.name.replace(f"_{region}", "").replace(region, "")
                if target.exists() and not overwrite:
                    print(f"  Skipped {csv_file.name} (file exists)")
                else:
                    shutil.copy2(str(csv_file), str(target))
                    print(f"  {'Overwrote' if target.exists() else 'Moved'} {csv_file.name} → {region}/")
    
    # Clean up temp extraction directory
    try:
        shutil.rmtree(source_dir)
    except Exception as e:
        print(f"Warning: Could not remove temp directory {source_dir}: {e}")
    
    print("Data organization complete!")

def download_and_extract(url: str, zip_path: Path, extract_to: Path) -> None:
    """Download, extract, and organize data files."""
    # Check if data directory exists and has content
    data_exists = extract_to.exists() and any(extract_to.iterdir())
    overwrite = True
    
    if data_exists:
        overwrite = confirm_overwrite()
        if not overwrite:
            print("Download cancelled. Existing data preserved.")
            return
    
    download(url, zip_path)
    temp_source = extract_zip(zip_path, extract_to)
    organize_data_files(temp_source, overwrite=overwrite)

if __name__ == "__main__":
    download_and_extract(URL, ZIP_PATH, EXTRACT_DIR)
