"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/cog/02_process_pisa_2018.py
RESEARCHERS: Dr. José Aparecido da Silva
             Me. Cássio Dalbem Barth
DATE:        2026-01-06

DESCRIPTION: 
    Processes PISA 2018 student data (Region Level).
    - Robust filtering for Brazil (removes ALB, VNM, etc.).
    - Decodes MACRO-REGIONS using STRATUM string patterns.
    - Calculates Mean Scores (Math, Reading, Science).
    - EXPORTS: 
        1. Processed Dataset (data/processed/)
        2. Summary Report Table (reports/tables/)

DATA SOURCE CITATION:
    Title:      PISA 2018 Database
    Author:     Organisation For Economic Co-Operation and Development (OECD)
    Publisher:  Zenodo
    Date:       2019
    DOI:        10.5281/zenodo.13383223
    URL:        https://zenodo.org/records/13383223
    File:       CY07_MSU_STU_QQQ.sav
    
    Index
    Region: The Macro-Region name (e.g., South, Southeast, North, etc.).
    
    Data 
    Columns
    Student_Count: The total number of students ($N$) in the sample for that region.
    Math_Mean: The average score for Mathematics.
    Read_Mean: The average score for Reading.
    Science_Mean: The average score for Science.
    Cognitive_Global_Mean: The calculated composite average of Math, Reading, and Science.
    
    
"""
"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/cog/02_process_pisa_2015.py
RESEARCHERS: Dr. José Aparecido da Silva
             Me. Cássio Dalbem Barth
DATE:        2026-01-06

DESCRIPTION: 
    Processes PISA 2015 student data (State & Region Level).
    - Robust filtering for Brazil.
    - Decodes STATES (UF) from SUBNATIO/STRATUM codes.
    - Maps STATES to MACRO-REGIONS.
    - Calculates Mean Scores (Math, Reading, Science).
    - EXPORTS: 
        1. Processed Dataset (data/processed/)
        2. Summary Report Table (reports/varcog/)

DATA SOURCE CITATION:
    Title:      PISA 2015 Database
    Author:     OECD
    Publisher:  Zenodo
    Date:       2016
    File:       CY6_MS_CMB_STU_QQQ.sav
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
RAW_FILE = PROJECT_ROOT / 'data' / 'raw' / 'Pisa' / 'pisa_2015' / 'CY6_MS_CMB_STU_QQQ.sav'


# Outputs
REPORT_DIR = PROJECT_ROOT / 'reports' / 'varcog'
PATH_CSV = REPORT_DIR / 'data' / 'processed' / 'pisa_2015_states.csv'
PATH_XLSX = REPORT_DIR / 'xlsx' / 'pisa_2015_states.xlsx'

# --- MAPPINGS ---
# 1. IBGE Code to State (UF)
IBGE_CODE_MAP = {
    '11': 'RO', '12': 'AC', '13': 'AM', '14': 'RR', '15': 'PA', '16': 'AP', '17': 'TO',
    '21': 'MA', '22': 'PI', '23': 'CE', '24': 'RN', '25': 'PB', '26': 'PE', '27': 'AL', '28': 'SE', '29': 'BA',
    '31': 'MG', '32': 'ES', '33': 'RJ', '35': 'SP', # Note: 34 does not exist
    '41': 'PR', '42': 'SC', '43': 'RS',
    '50': 'MS', '51': 'MT', '52': 'GO', '53': 'DF'
}

# 2. State (UF) to Macro-Region
UF_REGION_MAP = {
    # North
    'RO': 'North', 'AC': 'North', 'AM': 'North', 'RR': 'North', 
    'PA': 'North', 'AP': 'North', 'TO': 'North',
    # Northeast
    'MA': 'Northeast', 'PI': 'Northeast', 'CE': 'Northeast', 'RN': 'Northeast', 
    'PB': 'Northeast', 'PE': 'Northeast', 'AL': 'Northeast', 'SE': 'Northeast', 'BA': 'Northeast',
    # Southeast
    'MG': 'Southeast', 'ES': 'Southeast', 'RJ': 'Southeast', 'SP': 'Southeast',
    # South
    'PR': 'South', 'SC': 'South', 'RS': 'South',
    # Center-West
    'MS': 'Center-West', 'MT': 'Center-West', 'GO': 'Center-West', 'DF': 'Center-West'
}

def setup_directories():
    for p in [PATH_CSV.parent, PATH_XLSX.parent]:
        p.mkdir(parents=True, exist_ok=True)

def process_pisa_2015():
    print(f"[INFO] Loading PISA 2015: {RAW_FILE.name}")
    
    # SUBNATIO often holds the State Code (e.g., BR-SP) directly in 2015
    cols = ['CNT', 'STRATUM', 'SUBNATIO', 'PV1MATH', 'PV1READ', 'PV1SCIE']
    
    try:
        # Load data (convert_categoricals=False helps read raw codes if needed, 
        # but 2015 usually works well with default if we look for strings)
        df = pd.read_spss(RAW_FILE, usecols=cols)
        
        # --- ROBUST FILTERING ---
        # Convert CNT to string and look for 'Brazil' or 'BRA'
        mask = df['CNT'].astype(str).str.contains('Brazil|BRA', case=False, na=False)
        df = df[mask].copy()
        
        print(f"       - Brazil rows found: {len(df)}")
        if len(df) == 0:
            print("[ERROR] No Brazil rows found. Check 'CNT' column content.")
            return None

    except Exception as e:
        print(f"[ERROR] {e}")
        return None

    print("[INFO] Mapping States and Regions...")

    def get_uf(row):
        # 1. Try SUBNATIO first (Best source)
        sub = str(row['SUBNATIO'])
        for code, uf in IBGE_CODE_MAP.items():
            if code in sub: return uf
            
        # 2. Try STRATUM (Backup)
        strat = str(row['STRATUM'])
        for code, uf in IBGE_CODE_MAP.items():
            if f"stratum {code}" in strat or f"BRA{code}" in strat:
                return uf
                
        return 'UNKNOWN'

    # 1. Create UF Column
    df['UF'] = df.apply(get_uf, axis=1)
    
    # Validation
    unknowns = df[df['UF'] == 'UNKNOWN']
    if len(unknowns) > 0:
        print(f"[WARNING] {len(unknowns)} rows unmapped. Sample Stratum: {unknowns['STRATUM'].iloc[0]}")
    
    df = df[df['UF'] != 'UNKNOWN']

    # 2. Create Region Column
    df['Region'] = df['UF'].map(UF_REGION_MAP)

    # --- AGGREGATE ---
    # Group by BOTH Region and UF to keep the hierarchy
    res = df.groupby(['Region', 'UF'])[['PV1MATH', 'PV1READ', 'PV1SCIE']].mean().reset_index()
    
    # Calculate Cognitive Global Mean
    res['Cognitive_Global_Mean'] = (res['PV1MATH'] + res['PV1READ'] + res['PV1SCIE']) / 3
    
    # Renaming columns for consistency
    res = res.rename(columns={
        'PV1MATH': 'Math_Mean',
        'PV1READ': 'Read_Mean',
        'PV1SCIE': 'Science_Mean'
    })
    
    return res.round(2).sort_values('Cognitive_Global_Mean', ascending=False)

if __name__ == "__main__":
    setup_directories()
    
    if RAW_FILE.exists():
        df_final = process_pisa_2015()
        
        if df_final is not None and not df_final.empty:
            df_final.to_csv(PATH_CSV, index=False)
            df_final.to_excel(PATH_XLSX, index=False)
            print(f"[SUCCESS] CSV Saved: {PATH_CSV}")
            print(f"[SUCCESS] Excel Saved: {PATH_XLSX}")
            print("\n--- FIRST 10 ROWS (Sorted by Score) ---")
            print(df_final.head(10))
            
            # Optional: Print Regional Summary as well
            print("\n--- REGIONAL SUMMARY ---")
            print(df_final.groupby('Region')[['Cognitive_Global_Mean']].mean().sort_values('Cognitive_Global_Mean', ascending=False))
            
    else:
        print(f"[ERROR] File not found: {RAW_FILE}")