#!/usr/bin/env python
"""
Master script to generate all paper maps for Vancouver golf census analysis.

This script orchestrates the complete pipeline:
1. Builds analytic tracts dataset
2. Computes census metrics
3. Calculates course exposure metrics
4. Generates static quintile maps with color-coded golf courses

All outputs are saved to outputs/paper_figures/maps/
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def run_script(script_name: str, description: str) -> None:
    """Run a prerequisite script and check for errors."""
    script_path = ROOT / script_name
    print(f"\n{'='*70}")
    print(f"Step: {description}")
    print(f"Running: {script_name}")
    print('='*70)
    
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=ROOT,
            check=True,
            capture_output=False,
        )
        print(f"[OK] {description} completed successfully\n")
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Error running {script_name}")
        print(f"Exit code: {e.returncode}")
        sys.exit(1)


def main() -> None:
    """Run the complete pipeline to generate all maps."""
    print("\n" + "="*70)
    print("VANCOUVER GOLF CENSUS - MAP GENERATION PIPELINE")
    print("="*70)
    
    # Step 1: Build analytic tracts with census data
    run_script(
        "paper_build_analytic_tracts_vancouver.py",
        "Building analytic census tracts dataset"
    )
    
    # Step 2: Calculate census metrics
    run_script(
        "paper_metrics_vancouver.py",
        "Computing census metrics"
    )
    
    # Step 3: Calculate course exposure metrics
    run_script(
        "paper_build_course_exposure_vancouver.py",
        "Calculating course exposure metrics"
    )
    
    # Step 4: Generate static maps with color-coded courses
    run_script(
        "paper_make_static_maps_vancouver.py",
        "Generating static quintile maps"
    )
    
    print("\n" + "="*70)
    print("[SUCCESS] ALL MAPS GENERATED SUCCESSFULLY")
    print("="*70)
    print(f"\nOutput location: {ROOT}/outputs/paper_figures/maps/")
    print("\nGenerated maps:")
    print("  - map_income_quintiles.png/.pdf")
    print("  - map_renters_quintiles.png/.pdf")
    print("  - map_seniors_quintiles.png/.pdf")
    print("  - map_population_quintiles.png/.pdf")
    print("  - map_density_quintiles.png/.pdf")
    print("\nEach map shows:")
    print("  * Census tract data in quintile color gradients")
    print("  * Golf courses color-coded by access type:")
    print("    - Red: Private courses")
    print("    - Purple: Municipal courses")
    print("    - Blue: Public courses")
    print("    - Gray: Unclassified courses")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()
