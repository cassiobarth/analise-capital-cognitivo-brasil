"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/cog/04_process_enem_triennium.py
RESEARCHERS: Dr. José Aparecido da Silva
             Me. Cássio Dalbem Barth
DATE:        2026-01-08 (Updated Paths)

DESCRIPTION: 
    Processes ENEM Microdata for the triennium (2022, 2023, 2024).
    - EXPORTS TO ENGINEERING: data/processed/enem_table_{year}.csv
    - EXPORTS TO ANALYTICS:   reports/varcog/xlsx/enem_consolidated_states_triennium.xlsx
"""
import pandas as pd
import numpy as np
import zipfile
import os
from pathlib import Path

# --- CONFIGURATION ---
YEARS = [2022, 2023, 2024]

# Path Setup
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent.parent

# 1. Inputs (Raw Data)
INPUT_DIR = PROJECT_ROOT / 'data' / 'raw'

# 2. Engineering Outputs (CSV for Machines)
PROC_DIR = PROJECT_ROOT / 'data' / 'processed'

# 3. Reporting Outputs (Excel for Humans)
REPORT_XLSX_DIR = PROJECT_ROOT / 'reports' / 'varcog' / 'xlsx'

# Columns to extract
COLS_ENEM = ['SG_UF_PROVA', 'NU_NOTA_CN', 'NU_NOTA_CH', 'NU_NOTA_LC', 'NU_NOTA_MT', 'NU_NOTA_REDACAO']

# Mapping to English
NAME_MAP = {
    'NU_NOTA_LC': 'Language', 
    'NU_NOTA_CH': 'Humanities', 
    'NU_NOTA_CN': 'Natural_Sciences', 
    'NU_NOTA_MT': 'Math', 
    'NU_NOTA_REDACAO': 'Essay'
}

# Region Mapping
UF_REGION_MAP = {
    'RO': 'North', 'AC': 'North', 'AM': 'North', 'RR': 'North', 'PA': 'North', 'AP': 'North', 'TO': 'North',
    'MA': 'Northeast', 'PI': 'Northeast', 'CE': 'Northeast', 'RN': 'Northeast', 'PB': 'Northeast', 
    'PE': 'Northeast', 'AL': 'Northeast', 'SE': 'Northeast', 'BA': 'Northeast',
    'MG': 'Southeast', 'ES': 'Southeast', 'RJ': 'Southeast', 'SP': 'Southeast',
    'PR': 'South', 'SC': 'South', 'RS': 'South',
    'MS': 'Center-West', 'MT': 'Center-West', 'GO': 'Center-West', 'DF': 'Center-West'
}

def setup_directories():
    """Ensures output directories exist."""
    for p in [PROC_DIR, REPORT_XLSX_DIR]:
        p.mkdir(parents=True, exist_ok=True)

def process_triennium():
    setup_directories()
    national_summary = []

    print("[INFO] Starting ENEM Triennium Processing...")

    for year in YEARS:
        zip_file = INPUT_DIR / f'microdados_enem_{year}.zip'
        
        if not zip_file.exists():
            print(f"[WARNING] File not found: {zip_file}")
            continue

        print(f"\n[INFO] Processing year {year}...")
        
        try:
            with zipfile.ZipFile(zip_file, 'r') as z:
                # Find CSV
                csv_files = [info for info in z.infolist() if info.filename.lower().endswith('.csv')]
                if not csv_files:
                    print(f"[ERROR] No CSV found inside zip for {year}.")
                    continue
                
                target_csv_info = sorted(csv_files, key=lambda x: x.file_size, reverse=True)[0]
                target_filename = target_csv_info.filename
                
                print(f"       - Target file: {target_filename} ({target_csv_info.file_size / 1e9:.2f} GB)")

                chunks_list = []
                with z.open(target_filename) as f:
                    # Using latin-1 is standard for ENEM
                    reader = pd.read_csv(f, sep=';', encoding='latin-1', usecols=COLS_ENEM, chunksize=500000)
                    for i, chunk in enumerate(reader):
                        chunk = chunk.dropna(subset=['NU_NOTA_CN', 'NU_NOTA_CH', 'NU_NOTA_LC', 'NU_NOTA_MT', 'NU_NOTA_REDACAO'])
                        if not chunk.empty:
                            chunks_list.append(chunk)
                        if i % 10 == 0:
                            print(f"       - Processing chunk {i}...")

                if not chunks_list:
                    print(f"[WARNING] Year {year} yielded no valid data.")
                    continue

                df_full = pd.concat(chunks_list)

                # 1. State Aggregation
                df_state = df_full.groupby('SG_UF_PROVA').mean().reset_index()
                
                # Metrics
                df_state['Mean_General'] = df_state[['NU_NOTA_CN', 'NU_NOTA_CH', 'NU_NOTA_LC', 'NU_NOTA_MT', 'NU_NOTA_REDACAO']].mean(axis=1)
                df_state['Std_Dev'] = df_state[['NU_NOTA_CN', 'NU_NOTA_CH', 'NU_NOTA_LC', 'NU_NOTA_MT', 'NU_NOTA_REDACAO']].std(axis=1)
                
                # Rename Columns
                rename_dict = NAME_MAP.copy()
                rename_dict['SG_UF_PROVA'] = 'UF'
                df_state = df_state.rename(columns=rename_dict)
                
                # Add Region Column
                df_state['Region'] = df_state['UF'].map(UF_REGION_MAP)
                
                # FORCE COLUMN ORDER
                cols = ['Region', 'UF', 'Mean_General', 'Std_Dev', 'Math', 'Language', 'Humanities', 'Natural_Sciences', 'Essay']
                df_state = df_state[cols].sort_values(by='Mean_General', ascending=False)
                
                # 1. SAVE TO PROCESSED (CSV)
                csv_proc_path = PROC_DIR / f'enem_table_{year}.csv'
                df_state.to_csv(csv_proc_path, sep=';', encoding='utf-8-sig', index=False)
                print(f"[ENGINEERING] Saved processed CSV: {csv_proc_path}")

                # 2. SAVE TO REPORTS (XLSX)
                xlsx_report_path = REPORT_XLSX_DIR / f'enem_table_{year}.xlsx'
                df_state.to_excel(xlsx_report_path, index=False)
                print(f"[ANALYTICS] Saved Excel Report: {xlsx_report_path}")

                # National Summary Data Collection
                nat_means = df_full[['NU_NOTA_CN', 'NU_NOTA_CH', 'NU_NOTA_LC', 'NU_NOTA_MT', 'NU_NOTA_REDACAO']].mean()
                summary_row = nat_means.to_dict()
                summary_row['Year'] = year
                summary_row['Valid_Participants'] = len(df_full)
                national_summary.append(summary_row)

        except Exception as e:
            print(f"[ERROR] Failure processing {year}: {e}")

    # Save National Summary
    if national_summary:
        df_nat = pd.DataFrame(national_summary).set_index('Year').rename(columns=NAME_MAP)
        df_nat['National_Mean'] = df_nat[['Language', 'Humanities', 'Natural_Sciences', 'Math', 'Essay']].mean(axis=1)
        
        path_nat_csv = PROC_DIR / 'enem_national_summary.csv'
        df_nat.to_csv(path_nat_csv, sep=';', encoding='utf-8-sig')
        print(f"\n[INFO] National summary saved to {path_nat_csv}")

def consolidate_states():
    print("\n[INFO] Starting Consolidation (Triennium State Analysis)...")
    
    dfs_years = []
    
    for year in YEARS:
        # Read from PROCESSED directory where we just saved them
        file_path = PROC_DIR / f'enem_table_{year}.csv'
        
        if file_path.exists():
            df = pd.read_csv(file_path, sep=';')
            
            if 'UF' not in df.columns:
                 df = df.rename(columns={'SG_UF_PROVA': 'UF'})

            # Select relevant columns for consolidation
            df = df[['UF', 'Mean_General']].rename(columns={
                'Mean_General': f'Mean_{year}'
            })
            dfs_years.append(df)
        else:
            print(f"[WARNING] Missing processed file for {year}. Run process_triennium() first.")

    if not dfs_years:
        return

    # Merge
    df_final = dfs_years[0]
    for df_year in dfs_years[1:]:
        df_final = df_final.merge(df_year, on='UF', how='outer')

    # Add Region Mapping
    df_final['Region'] = df_final['UF'].map(UF_REGION_MAP)

    # Calculate Triennium Mean
    mean_cols = [c for c in df_final.columns if c.startswith('Mean_')]
    df_final['Triennium_Mean'] = df_final[mean_cols].mean(axis=1)

    # Final Column Order
    cols_order = ['Region', 'UF', 'Triennium_Mean'] + mean_cols
    df_final = df_final[cols_order].sort_values(by='Triennium_Mean', ascending=False)

    # 1. SAVE TO PROCESSED (CSV)
    proc_out = PROC_DIR / 'enem_consolidated_states_triennium.csv'
    df_final.to_csv(proc_out, index=False)
    print(f"[ENGINEERING] Consolidated CSV saved: {proc_out}")

    # 2. SAVE TO REPORTS (XLSX)
    report_out = REPORT_XLSX_DIR / 'enem_consolidated_states_triennium.xlsx'
    try:
        df_final.to_excel(report_out, index=False)
        print(f"[ANALYTICS] Consolidated Excel saved: {report_out}")
        print("\n--- TOP 5 STATES (Triennium) ---")
        print(df_final.head().to_string(index=False))
    except Exception as e:
        print(f"[ERROR] Failed to save Excel: {e}")

if __name__ == "__main__":
    process_triennium()
    consolidate_states()