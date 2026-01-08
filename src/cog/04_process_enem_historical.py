"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/cog/04_process_enem_historical.py
RESEARCHERS: Dr. Jose Aparecido da Silva
             Me. Cassio Dalbem Barth
DATE:        2026-01-08 (Fix v2.1: Zero-Inflation Handler)

DESCRIPTION:
    Extracts historical ENEM data (2015, 2018) from raw microdata zips.
    
    CRITICAL FIXES (v2.1):
    1. Zero Handling: Converts scores of 0.0 to NaN before averaging. 
       This prevents absent days (Score=0) from artificially dragging down 
       the state averages (The "Range < 400" bug).
    2. UF Priority: Refined column detection to prefer Residence over School State.

INPUT:
    - data/raw/microdados_enem_2015.zip
    - data/raw/microdados_enem_2018.zip

OUTPUT:
    - data/processed/enem_table_2015.csv / .xlsx
    - data/processed/enem_table_2018.csv / .xlsx
"""

import pandas as pd
import zipfile
import os
import sys
import numpy as np
from pathlib import Path

# --- 1. SAFEGUARD IMPORT PROTOCOL ---
script_dir = os.path.dirname(os.path.abspath(__file__))
lib_path = os.path.join(script_dir, 'lib')
if lib_path not in sys.path: sys.path.append(lib_path)

try:
    from safeguard import DataGuard
except ImportError:
    DataGuard = None

# --- 2. CONFIGURATION ---
BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_RAW = os.path.join(BASE_PATH, 'data', 'raw')
DATA_PROCESSED = os.path.join(BASE_PATH, 'data', 'processed')
REPORT_XLSX = os.path.join(BASE_PATH, 'reports', 'varcog', 'xlsx')

os.makedirs(DATA_PROCESSED, exist_ok=True)
os.makedirs(REPORT_XLSX, exist_ok=True)

YEARS = [2015, 2018]

# Priority List: Residence > School (Esc) > Generic
POSSIBLE_UF_COLS = [
    'SG_UF_RESIDENCIA', 'UF_RESIDENCIA', 'NO_UF_RESIDENCIA', # Residence (Best)
    'SG_UF_ESC', 'UF_ESC', 'NO_UF_ESC'                       # School (Fallback)
]
POSSIBLE_SCORES = ['NU_NOTA_CN', 'NU_NOTA_CH', 'NU_NOTA_LC', 'NU_NOTA_MT', 'NU_NOTA_REDACAO']

REGIONAL_MAP = {
    'N': ['AC','AP','AM','PA','RO','RR','TO'],
    'NE': ['AL','BA','CE','MA','PB','PE','PI','RN','SE'],
    'CO': ['DF','GO','MT','MS'],
    'SE': ['ES','MG','RJ','SP'],
    'S': ['PR','RS','SC']
}
UF_TO_REGION = {uf: r for r, ufs in REGIONAL_MAP.items() for uf in ufs}

def identify_columns(header):
    """Dynamically finds the UF column and available Score columns."""
    # Find UF based on priority list
    uf_col = next((c for c in POSSIBLE_UF_COLS if c in header), None)
    
    # Find Scores
    score_cols = [c for c in POSSIBLE_SCORES if c in header]
    
    return uf_col, score_cols

def process_year(year):
    print("="*60)
    print(f"[START] Processing ENEM {year}")
    print("="*60)
    
    zip_filename = f"microdados_enem_{year}.zip"
    zip_path = os.path.join(DATA_RAW, zip_filename)
    
    if not os.path.exists(zip_path):
        print(f"[SKIP] File not found: {zip_path}")
        return

    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            # Smart file finder (ignores 'ITENS' or docs)
            csv_file = next((f for f in z.namelist() 
                             if f.endswith('.csv') and 'MICRODADOS' in f and 'ITENS' not in f), None)
            
            if not csv_file:
                # Fallback: take the largest CSV
                csv_files = [f for f in z.namelist() if f.endswith('.csv')]
                if not csv_files:
                    print("[ERROR] No CSV found inside zip.")
                    return
                # Sort by size (largest is likely the data)
                csv_file = sorted(csv_files, key=lambda x: z.getinfo(x).file_size, reverse=True)[0]
            
            print(f"[FILE] Target: {csv_file}")
            
            # 1. READ HEADER
            try:
                # Try semicolon first
                header = pd.read_csv(z.open(csv_file), sep=';', nrows=0, encoding='latin1').columns.tolist()
                sep = ';'
            except:
                header = pd.read_csv(z.open(csv_file), sep=',', nrows=0, encoding='latin1').columns.tolist()
                sep = ','
            
            uf_col, score_cols = identify_columns(header)
            
            if not uf_col:
                print(f"[CRITICAL] UF Column not found. Header sample: {header[:10]}")
                return
            
            if not score_cols:
                print("[CRITICAL] No score columns found.")
                return

            print(f"[INFO] Structure detected:")
            print(f"       - Separator: '{sep}'")
            print(f"       - UF Column: {uf_col}")
            print(f"       - Scores: {len(score_cols)} variables")

            # 2. CHUNK PROCESSING
            use_cols = [uf_col] + score_cols
            chunk_size = 100000
            agg_data = {} 
            
            print("[INFO] Stream processing started...")
            
            reader = pd.read_csv(z.open(csv_file), sep=sep, encoding='latin1', 
                               usecols=use_cols, chunksize=chunk_size)
            
            batch_count = 0
            for chunk in reader:
                batch_count += 1
                if batch_count % 20 == 0:
                    print(f"       - Processed {batch_count * chunk_size // 1000}k rows...", end='\r')
                
                # A. Convert to numeric
                for col in score_cols:
                    chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
                
                # B. CRITICAL FIX: Treat 0.0 as NaN
                # This prevents students absent in one area from dragging down the average
                chunk[score_cols] = chunk[score_cols].replace(0, np.nan)
                
                # C. Calculate Student Mean (ignoring NaNs)
                chunk['Student_Avg'] = chunk[score_cols].mean(axis=1)
                
                # D. Drop students with NO scores (all NaNs)
                chunk = chunk.dropna(subset=['Student_Avg'])
                
                # Group
                grouped = chunk.groupby(uf_col)['Student_Avg'].agg(['sum', 'count'])
                
                for uf, row in grouped.iterrows():
                    if uf not in agg_data:
                        agg_data[uf] = {'sum': 0.0, 'count': 0}
                    agg_data[uf]['sum'] += row['sum']
                    agg_data[uf]['count'] += row['count']

            print(f"\n[INFO] Aggregation complete. Processed approx {batch_count * chunk_size} rows.")

            # 3. CONSOLIDATE
            results = []
            for uf, metrics in agg_data.items():
                if metrics['count'] > 0:
                    mean_val = metrics['sum'] / metrics['count']
                    results.append({'UF': uf, 'Mean_General': mean_val, 'Student_Count': metrics['count']})
            
            df_final = pd.DataFrame(results)
            
            # Map Region
            df_final['Region'] = df_final['UF'].map(UF_TO_REGION)
            
            # Sort
            df_final = df_final.sort_values('Mean_General', ascending=False)
            df_final = df_final[['Region', 'UF', 'Mean_General', 'Student_Count']]

            # 4. SAFEGUARD
            if DataGuard:
                print("[AUDIT] Running DataGuard...")
                guard = DataGuard(df_final, f"ENEM {year}")
                # Now that zeros are handled, we expect means > 450
                guard.check_range(['Mean_General'], 450, 700) 
                guard.check_historical_consistency('Mean_General', 'UF')
                guard.validate(strict=True)

            # 5. EXPORT
            csv_path = os.path.join(DATA_PROCESSED, f'enem_table_{year}.csv')
            xlsx_path = os.path.join(REPORT_XLSX, f'enem_table_{year}.xlsx')
            
            df_final.to_csv(csv_path, index=False)
            df_final.to_excel(xlsx_path, index=False)
            
            print(f"[SUCCESS] Saved: {csv_path}")

    except Exception as e:
        print(f"[CRITICAL ERROR] {e}")
        # import traceback; traceback.print_exc()

if __name__ == "__main__":
    for y in YEARS:
        process_year(y)