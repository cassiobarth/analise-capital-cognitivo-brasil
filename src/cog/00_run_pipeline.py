"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/cog/00_run_pipeline.py
RESEARCHERS: Dr. José Aparecido da Silva
             Me. Cássio Dalbem Barth
DATE:        2026-01-08 (Architecture Update)

DESCRIPTION: 
    Master Pipeline Orchestrator.
    Executes the full End-to-End Data Pipeline in the correct dependency order.
    
    [PHASE 1: EXTRACTION & PROCESSING]
    - Extracts raw data and converts to clean CSVs in 'data/processed'.
    - Covers both Historical (2015-2018) and Recent (2022-2024) waves.

    [PHASE 2: CONSOLIDATION]
    - Merges the disparate CSVs into a single Longitudinal Panel.
    
    [PHASE 3: ANALYTICS & VISUALIZATION]
    - Generates correlation matrices and final charts in 'reports/'.
    
    WARNING: This process is memory intensive. Ensure all raw ZIPs are present.
"""
import subprocess
import sys
import time
from pathlib import Path

# --- CONFIGURATION ---
# Define the project root based on the location of this script
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent.parent

# List of scripts to execute in VALID LOGICAL ORDER
# Paths are relative to the project root
PIPELINE_SCRIPTS = [
    # ------------------------------------------------------------------
    # PHASE 1: INDIVIDUAL EXTRACTION (Raw -> Processed CSV)
    # ------------------------------------------------------------------
    
    # 1. SAEB (School Performance)
    "src/cog/01_process_saeb_historical.py",      # Extracts 2015 & 2017
    "src/cog/01_process_saeb_2023_uf_region.py",  # Extracts 2023
    
    # 2. PISA (International Baseline)
    "src/cog/02_process_pisa_2015_uf_region.py",  # Extracts 2015
    "src/cog/02_process_pisa_2018_region.py",     # Extracts 2018
    "src/cog/02_process_pisa_2022_region.py",     # Extracts 2022
    
    # 3. ENEM (National Exam)
    "src/cog/04_process_enem_historical.py",      # Extracts 2015 & 2018
    "src/cog/04_process_enem_triennium.py",       # Extracts 2022, 2023, 2024
    
    # ------------------------------------------------------------------
    # PHASE 2: CONSOLIDATION (Processed CSVs -> Master Panel)
    # ------------------------------------------------------------------
    # Merges all above into 'data/processed/panel_longitudinal_waves.csv'
    # CRITICAL: This script depends on all previous scripts succeeding.
    "src/cog/03_consolidate_longitudinal_panel.py",
    
    # ------------------------------------------------------------------
    # PHASE 3: ANALYTICS & INSIGHTS (Master Panel -> Reports)
    # ------------------------------------------------------------------
    # Calculates Pearson/Spearman correlations
    "src/cog/05_correlate_pearson_spearman.py",
    # Generates Heatmaps and Scatterplots
    "src/cog/06_visualize_correlations.py"
]

def run_script(script_path):
    """Executes a single python script via subprocess."""
    full_path = PROJECT_ROOT / script_path
    
    if not full_path.exists():
        print(f"[ERROR] Script not found: {script_path}")
        return False

    print(f"\n" + "="*60)
    print(f"[START] Running: {script_path}")
    print("="*60 + "\n")

    start_time = time.time()
    
    try:
        # Run the script and wait for it to finish
        # sys.executable ensures we use the same python interpreter (virtualenv)
        result = subprocess.run(
            [sys.executable, str(full_path)], 
            check=True,
            capture_output=False  # Let the script print directly to console
        )
        
        elapsed = time.time() - start_time
        print(f"\n[DONE] Finished: {script_path}")
        print(f"[TIME] Duration: {elapsed:.2f} seconds")
        return True

    except subprocess.CalledProcessError as e:
        print(f"\n[FAILURE] Script crashed: {script_path}")
        print(f"[EXIT CODE] {e.returncode}")
        return False
    except KeyboardInterrupt:
        print("\n[ABORTED] Pipeline stopped by user.")
        sys.exit(1)

def main():
    total_start = time.time()
    success_count = 0
    
    print("Starting Cognitive Capital Data Pipeline...")
    print(f"Project Root: {PROJECT_ROOT}")
    print(f"Scripts to run: {len(PIPELINE_SCRIPTS)}")
    
    for script in PIPELINE_SCRIPTS:
        success = run_script(script)
        if success:
            success_count += 1
        else:
            print(f"[WARNING] Pipeline failed at step: {script}")
            
            # CRITICAL CHECK:
            # If the Consolidation script (03) fails, the subsequent Analytics scripts (05, 06)
            # will definitely fail because the input file won't exist. It's safer to stop.
            if "03_consolidate" in script:
                print("\n[CRITICAL ERROR] Consolidation failed. Stopping downstream analytics.")
                print("Please check the logs above, fix the error, and re-run.")
                break

    total_elapsed = time.time() - total_start
    print("\n" + "="*60)
    print("PIPELINE EXECUTION SUMMARY")
    print("="*60)
    print(f"Total Scripts Queued: {len(PIPELINE_SCRIPTS)}")
    print(f"Successful:           {success_count}")
    print(f"Failed/Skipped:       {len(PIPELINE_SCRIPTS) - success_count}")
    print(f"Total Time:           {total_elapsed / 60:.2f} minutes")
    print("="*60)

if __name__ == "__main__":
    main()