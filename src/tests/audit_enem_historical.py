"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/tests/audit_enem_historical.py
TYPE:        Quality Assurance (QA) / Forensic Audit
DATE:        2026-01-08

DESCRIPTION:
    Forensic audit script for ENEM Historical Data.
    It verifies if the extraction scripts produced consistent results 
    before they enter the correlation matrix.

CHECKS:
    1. Score Boundaries: Are grades within 0-1000?
    2. Geographic Consistency: Does the regional ranking match historical patterns?
       (Prevents the "North > South" labeling error).

USAGE:
    python src/tests/audit_enem_historical.py
"""

import pandas as pd
import os
import sys

# --- SETUP: Import DataGuard ---
# Logic: We are in 'src/tests'. We need 'src/cog/lib'.
current_dir = os.path.dirname(os.path.abspath(__file__)) # .../src/tests
src_dir = os.path.dirname(current_dir)                  # .../src
lib_path = os.path.join(src_dir, 'cog', 'lib')           # .../src/cog/lib

if lib_path not in sys.path:
    sys.path.append(lib_path)

try:
    from safeguard import DataGuard
    print("[INIT] DataGuard module loaded successfully.")
except ImportError:
    print(f"[CRITICAL] Could not import 'safeguard'.")
    print(f"Expected location: {lib_path}")
    sys.exit(1)

# --- CONFIGURATION ---
# Project root is parent of 'src'
PROJECT_ROOT = os.path.dirname(src_dir)
DATA_PROCESSED = os.path.join(PROJECT_ROOT, 'data', 'processed')

def audit_directory():
    print("="*60)
    print("ENEM HISTORICAL DATA AUDIT")
    print(f"Target Dir: {DATA_PROCESSED}")
    print("="*60)
    
    if not os.path.exists(DATA_PROCESSED):
        print(f"[ERROR] Directory not found: {DATA_PROCESSED}")
        return

    # Scan for any CSV that looks like ENEM
    all_files = os.listdir(DATA_PROCESSED)
    enem_candidates = [f for f in all_files if 'enem' in f.lower() and f.endswith('.csv')]
    
    if not enem_candidates:
        print("[WARNING] No ENEM files found in data/processed/")
        return

    for filename in enem_candidates:
        filepath = os.path.join(DATA_PROCESSED, filename)
        print(f"\n[AUDIT] Inspecting: {filename}")
        
        try:
            df = pd.read_csv(filepath)
            
            # 1. Identify Score Columns (Heuristic)
            score_cols = [c for c in df.columns if any(x in c.lower() for x in ['mean', 'nota', 'score', 'proficiencia'])]
            
            if not score_cols:
                print("   [SKIP] No obvious score columns found.")
                continue

            # 2. Run DataGuard
            guard = DataGuard(df, filename)
            
            # Check A: Range (ENEM is strictly 0-1000)
            guard.check_range(score_cols, 0, 1000)
            
            # Check B: Regional Consistency (The "Canary Test")
            geo_col = next((c for c in df.columns if c in ['UF', 'SG_UF', 'Region']), None)
            
            if geo_col:
                target_score = score_cols[0] 
                print(f"   - Consistency Check: '{target_score}' vs '{geo_col}'")
                guard.check_historical_consistency(target_score, geo_col)
            else:
                print("   [INFO] No Geographic column found. Skipping regional check.")

            # 3. Report Results (Strict=False lets us see all errors)
            guard.validate(strict=False)

        except Exception as e:
            print(f"   [ERROR] Failed to read/audit file: {e}")

    print("\n[DONE] Audit session finished.")

if __name__ == "__main__":
    audit_directory()