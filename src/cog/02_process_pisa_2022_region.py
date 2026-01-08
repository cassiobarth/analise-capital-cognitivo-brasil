"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/cog/02_process_pisa_2022_regional.py
RESEARCHERS: Dr. José Aparecido da Silva
             Me. Cássio Dalbem Barth
DATE:        2026-01-06

DESCRIPTION: 
    Processes PISA 2022 student data (Regional Level).
    - Filters for Brazil (BRA).
    - Decodes MACRO-REGIONS using STRATUM string patterns.
    - Calculates Student Count (N) and Mean Scores.
    - EXPORTS: 
        1. Processed Dataset (data/processed/) - *Optional if needed*
        2. Summary Report Tables to:
           - reports/varcog/csv/
           - reports/varcog/xlsx/

DATA SOURCE CITATION:
    Title:      PISA 2022 Database
    Author:     OECD
    Publisher:  OECD / Zenodo (Mirror)
    Date:       2023
    File:       CY08MSP_STU_QQQ.sav
"""
import pandas as pd
import numpy as np
import os
from pathlib import Path

# --- CONFIGURATION ---
SEED = 42
np.random.seed(SEED)

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent.parent
RAW_FILE = PROJECT_ROOT / 'data' / 'raw' / 'Pisa' / 'pisa_2022' / 'CY08MSP_STU_QQQ.sav'

# Outputs
REPORT_DIR = PROJECT_ROOT / 'reports' / 'varcog'
PATH_CSV = REPORT_DIR / 'data' / 'processed' / 'pisa_2022_regional_summary.csv'
PATH_XLSX = REPORT_DIR / 'xlsx' / 'pisa_2022_regional_summary.xlsx'

# --- MAPPINGS ---
# PISA 2022 Stratum logic for Brazil usually follows IBGE Region first digit
# 1=North, 2=Northeast, 3=Southeast, 4=South, 5=Center-West

def setup_directories():
    # Ensure all parent directories exist (CSV and XLSX)
    for p in [PATH_CSV.parent, PATH_XLSX.parent]:
        p.mkdir(parents=True, exist_ok=True)

def process_pisa_regional():
    print(f"[INFO] Loading PISA 2022: {RAW_FILE.name}")
    
    # Columns: Country, Stratum (contains region info), Scores
    cols = ['CNT', 'STRATUM', 'PV1MATH', 'PV1READ', 'PV1SCIE']
    
    try:
        # Requires 'pyreadstat' library installed
        df = pd.read_spss(RAW_FILE, usecols=cols)
    except Exception as e:
        print(f"[ERROR] Could not read file: {e}")
        return None

    # Filter Brazil (Robust: checks for 'BRA' OR 'Brazil')
    mask = df['CNT'].astype(str).str.contains('Brazil|BRA', case=False, na=False)
    df = df[mask].copy()
    print(f"       - Brazil rows found: {len(df)}")
    
    if len(df) == 0:
        print("[ERROR] No Brazil rows found. Check if CNT column uses 'BRA'.")
        return None

    # --- REGION DECODER (Option A: Text Matching) ---
    def get_region(stratum):
        s = str(stratum).upper() # Convert to uppercase for matching
        
        # Priority matches (Composite names first)
        if 'CENTRO-OESTE' in s or 'CENTRO OESTE' in s: return 'Center-West'
        if 'NORDESTE' in s: return 'Northeast'
        if 'SUDESTE' in s: return 'Southeast'
        
        # Single word matches (Check last to avoid partial matches)
        if 'NORTE' in s: return 'North'
        if 'SUL' in s: return 'South'
        
        return 'UNKNOWN'

    print("[INFO] Aggregating by Macro-Region...")
    df['Region'] = df['STRATUM'].apply(get_region)
    
    # Validation
    unknowns = df[df['Region'] == 'UNKNOWN']
    if len(unknowns) > 0:
        print(f"[WARNING] {len(unknowns)} rows unmapped. Sample Stratum: {unknowns['STRATUM'].iloc[0]}")

    # Remove Unknowns
    df = df[df['Region'] != 'UNKNOWN']
    
    # --- AGGREGATION ---
    # 1. Calculate Means
    means = df.groupby('Region')[['PV1MATH', 'PV1READ', 'PV1SCIE']].mean().reset_index()
    
    # 2. Calculate Student Count
    counts = df['Region'].value_counts().reset_index()
    counts.columns = ['Region', 'Student_Count']
    
    # 3. Merge
    res = pd.merge(counts, means, on='Region')
    
    # Calculate Global Mean
    res['Cognitive_Global_Mean'] = (res['PV1MATH'] + res['PV1READ'] + res['PV1SCIE']) / 3
    
    # Rename for consistency with 2018 format
    res = res.rename(columns={
        'PV1MATH': 'Math_Mean',
        'PV1READ': 'Read_Mean',
        'PV1SCIE': 'Science_Mean'
    })

    # Reorder columns to match 2018 Standard
    final_cols = ['Region', 'Student_Count', 'Math_Mean', 'Read_Mean', 'Science_Mean', 'Cognitive_Global_Mean']
    res = res[final_cols]

    return res.round(2).sort_values('Cognitive_Global_Mean', ascending=False)

if __name__ == "__main__":
    setup_directories()
    
    if RAW_FILE.exists():
        df_final = process_pisa_regional()
        
        if df_final is not None and not df_final.empty:
            # Save CSV
            df_final.to_csv(PATH_CSV, index=False)
            print(f"[SUCCESS] CSV Saved: {PATH_CSV}")
            
            # Save Excel
            df_final.to_excel(PATH_XLSX, index=False)
            print(f"[SUCCESS] Excel Saved: {PATH_XLSX}")
            
            print("\n--- REGIONAL SUMMARY (Sorted by Score) ---")
            print(df_final.to_string(index=False))
    else:
        print(f"[ERROR] File not found: {RAW_FILE}")