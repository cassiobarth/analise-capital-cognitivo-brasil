"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/cog/00_run_pipeline.py
RESEARCHERS: Dr. José Aparecido da Silva
             Me. Cássio Dalbem Barth
DATE:        2026-01-06

DESCRIPTION: 
    Master Pipeline Orchestrator.
    Executes all data processing scripts sequentially:
    1. SAEB 2023
    2. PISA (2015, 2018, 2022)
    3. ENEM Triennium (2022-2024)
    
    This ensures that all datasets are generated and updated in the correct order.
    
    WARNING: This process is memory intensive and may take considerable time 
    depending on the hardware, especially during the ENEM processing step.
"""
import subprocess
import sys
import time
from pathlib import Path

# --- CONFIGURATION ---
# Define the project root based on the location of this script
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent.parent

# List of scripts to execute in order
# Paths are relative to the project root
PIPELINE_SCRIPTS = [
    "src/cog/01_process_saeb_2023_uf_region.py",
    "src/cog/02_process_pisa_2015_uf_region.py",
    "src/cog/02_process_pisa_2018_region.py",
    "src/cog/02_process_pisa_2022_region.py",
    "src/cog/04_process_enem_triennium.py"
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
            print(f"[WARNING] Pipeline continuing, but {script} failed.")
            # Optional: break here if you want to stop on first error
            # break 

    total_elapsed = time.time() - total_start
    print("\n" + "="*60)
    print("PIPELINE EXECUTION SUMMARY")
    print("="*60)
    print(f"Total Scripts: {len(PIPELINE_SCRIPTS)}")
    print(f"Successful:    {success_count}")
    print(f"Failed:        {len(PIPELINE_SCRIPTS) - success_count}")
    print(f"Total Time:    {total_elapsed / 60:.2f} minutes")
    print("="*60)

if __name__ == "__main__":
    main()