import requests
import os
import zipfile
from pathlib import Path
from tqdm import tqdm

URL = "https://github.com/colerjstevenson/GolfGulf/releases/download/data/golf_data.zip"
ZIP_PATH = Path("data/golf_data.zip")
EXTRACT_DIR = Path("data")

def download(url: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(url, stream=True)
    response.raise_for_status()
    total = int(response.headers.get("content-length", 0))
    with open(path, "wb") as f, tqdm(total=total, unit="B", unit_scale=True, desc="Downloading") as bar:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
                bar.update(len(chunk))

def extract_zip(zip_path: Path, extract_to: Path, remove_zip: bool = True) -> None:
    extract_to.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as zf:
        zf.extractall(extract_to)

    # If the zip contained a top-level golf_data/ folder, flatten it
    nested_dir = extract_to / 'golf_data'
    if nested_dir.is_dir():
        for item in nested_dir.iterdir():
            target = extract_to / item.name
            if target.exists():
                # Skip moving if name collision; could log or handle differently
                continue
            item.rename(target)
        try:
            nested_dir.rmdir()
        except OSError:
            pass

    if remove_zip:
        try:
            zip_path.unlink()
        except OSError:
            pass

def download_and_extract(url: str, zip_path: Path, extract_to: Path) -> None:
    download(url, zip_path)
    extract_zip(zip_path, extract_to)

if __name__ == "__main__":
    download_and_extract(URL, ZIP_PATH, EXTRACT_DIR)
