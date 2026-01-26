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
import datetime
from pathlib import Path

# --- CONFIGURATION ---
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent.parent
LOG_DIR = PROJECT_ROOT / 'logs'
LOG_DIR.mkdir(exist_ok=True)

# List of scripts to execute in VALID LOGICAL ORDER
PIPELINE_SCRIPTS = [
    # ------------------------------------------------------------------
    # PHASE 1: INDIVIDUAL EXTRACTION (Raw -> Processed CSV)
    # ------------------------------------------------------------------
    "src/cog/01_process_saeb_historical.py",      # Extracts 2015 & 2017
    "src/cog/01_process_saeb_2023_uf_region.py",  # Extracts 2023
    
    "src/cog/02_process_pisa_2015_uf_region.py",  # Extracts 2015
    "src/cog/02_process_pisa_2018_region.py",     # Extracts 2018
    "src/cog/02_process_pisa_2022_region.py",     # Extracts 2022
    
    "src/cog/04_process_enem_historical.py",      # Extracts 2015 & 2018
    "src/cog/04_process_enem_triennium.py",       # Extracts 2022, 2023, 2024
    
    # ------------------------------------------------------------------
    # PHASE 2: CONSOLIDATION (Processed CSVs -> Master Panel)
    # ------------------------------------------------------------------
    "src/cog/03_consolidate_longitudinal_panel.py",
    
    # ------------------------------------------------------------------
    # PHASE 3: ANALYTICS & INSIGHTS (Master Panel -> Reports)
    # ------------------------------------------------------------------
    "src/cog/05_correlate_pearson_spearman.py",
    "src/cog/06_visualize_correlations.py"
]

def get_log_file_handle():
    """Creates a timestamped log file."""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"pipeline_execution_{timestamp}.log"
    return open(log_path, 'w', encoding='utf-8'), log_path

def log(file_handle, message, level="INFO"):
    """Writes to both console and log file."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted = f"[{timestamp}] [{level}] {message}"
    print(formatted)
    if file_handle:
        file_handle.write(formatted + "\n")
        file_handle.flush()

def run_script(script_rel_path, log_handle):
    """Executes a single python script via subprocess with logging."""
    full_path = PROJECT_ROOT / script_rel_path
    
    if not full_path.exists():
        log(log_handle, f"Script not found: {script_rel_path}", "ERROR")
        return False

    log(log_handle, "="*60, "SEP")
    log(log_handle, f"Running: {script_rel_path}", "START")
    
    start_time = time.time()
    
    try:
        # Capture output to log it to file as well
        result = subprocess.run(
            [sys.executable, str(full_path)],
            capture_output=True,
            text=True,
            encoding='utf-8',
            cwd=PROJECT_ROOT
        )
        
        # Stream stdout to log and console
        if result.stdout:
            print(result.stdout.strip())
            log_handle.write(result.stdout + "\n")
        
        # Stream stderr to log and console
        if result.stderr:
            print(f"[STDERR] {result.stderr.strip()}", file=sys.stderr)
            log_handle.write(f"[STDERR]\n{result.stderr}\n")

        if result.returncode == 0:
            elapsed = time.time() - start_time
            log(log_handle, f"Finished: {script_rel_path} ({elapsed:.2f}s)", "SUCCESS")
            return True
        else:
            log(log_handle, f"Script crashed: {script_rel_path} (Exit Code: {result.returncode})", "FAILURE")
            return False

    except Exception as e:
        log(log_handle, f"Execution error: {str(e)}", "CRITICAL")
        return False

def main():
    log_file, log_path = get_log_file_handle()
    
    try:
        log(log_file, "Starting Cognitive Capital Data Pipeline...", "INIT")
        log(log_file, f"Project Root: {PROJECT_ROOT}")
        log(log_file, f"Log File: {log_path}")
        
        total_start = time.time()
        success_count = 0
        
        for script in PIPELINE_SCRIPTS:
            success = run_script(script, log_file)
            
            if success:
                success_count += 1
            else:
                log(log_file, f"Pipeline step failed: {script}", "WARNING")
                
                # CRITICAL CHECK:
                # If consolidation fails, downstream analytics are impossible.
                if "03_consolidate" in script:
                    log(log_file, "Consolidation failed. Stopping downstream analytics.", "CRITICAL")
                    break

        total_elapsed = time.time() - total_start
        log(log_file, "="*60, "SEP")
        log(log_file, "PIPELINE EXECUTION SUMMARY", "SUMMARY")
        log(log_file, f"Total Scripts Queued: {len(PIPELINE_SCRIPTS)}")
        log(log_file, f"Successful:          {success_count}")
        log(log_file, f"Failed/Skipped:      {len(PIPELINE_SCRIPTS) - success_count}")
        log(log_file, f"Total Time:          {total_elapsed / 60:.2f} minutes")
        
    finally:
        log_file.close()

if __name__ == "__main__":
    main()