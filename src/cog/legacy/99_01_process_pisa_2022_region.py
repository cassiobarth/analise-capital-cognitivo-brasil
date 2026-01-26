"""
================================================================================
PROJECT:        COGNITIVE CAPITAL ANALYSIS - BRAZIL
SCRIPT:         src/cog/process_pisa_unified.py
VERSION:        8.0 (Final Production Version)
DATE:           2026-01-14
--------------------------------------------------------------------------------
PRINCIPAL INVESTIGATOR:  Dr. José Aparecido da Silva
LEAD DEVELOPER:          Specialist in Applied Statistics
SOURCE:                 OECD PISA Microdata (Cycles 2015, 2018, 2022)
================================================================================

ABSTRACT:
    Unified ETL pipeline for processing OECD PISA student-level microdata. 
    Handles architectural shifts across cycles while maintaining statistical 
    integrity. This is the official production version for the book project.

DATA SOURCE:
    - OECD PISA Raw Datasets (SPSS .sav format)
    - URL: https://www.oecd.org/pisa/data/
    - Access Date: 2026-01-14

RASTREABILITY SETTINGS:
    - INPUT_ROOT: data/raw/Pisa/
    - OUTPUT_CSV: data/processed/pisa_[year]_summary.csv
    - OUTPUT_XLSX: reports/varcog/xlsx/pisa_[year]_summary.xlsx

TABLE CONTENT:
    - Region/UF: Geographic identifier.
    - Math, Read, Science: Unweighted proficiency averages.
    - Cognitive_Global_Mean: Composite score (mean of the three core domains).
    - Student_Count: Sample size (N) per geographic unit.

DEPENDENCIES:
    pandas, numpy, pyreadstat, openpyxl, pathlib
================================================================================
"""

import pandas as pd
import numpy as np
import os
import sys
import time
import pyreadstat
from pathlib import Path
from datetime import timedelta

# --- 1. SETUP & PATHS ---
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent.parent
DATA_RAW_ROOT = PROJECT_ROOT / 'data' / 'raw' / 'Pisa'
DATA_PROCESSED_DIR = PROJECT_ROOT / 'data' / 'processed'
REPORT_DIR = PROJECT_ROOT / 'reports' / 'varcog' / 'xlsx'

for path in [DATA_PROCESSED_DIR, REPORT_DIR]:
    path.mkdir(parents=True, exist_ok=True)

# --- 2. EXPLICIT MAPPING DICTIONARIES ---
NAME_TO_IBGE = {
    'RONDONIA': 11, 'RONDÔNIA': 11, 'ACRE': 12, 'AMAZONAS': 13, 'RORAIMA': 14,
    'PARA': 15, 'PARÁ': 15, 'AMAPA': 16, 'AMAPÁ': 16, 'TOCANTINS': 17,
    'MARANHAO': 21, 'MARANHÃO': 21, 'PIAUI': 22, 'PIAUÍ': 22, 'CEARA': 23, 'CEARÁ': 23,
    'RIO GRANDE DO NORTE': 24, 'PARAIBA': 25, 'PARAÍBA': 25, 'PERNAMBUCO': 26,
    'ALAGOAS': 27, 'SERGIPE': 28, 'BAHIA': 29,
    'MINAS GERAIS': 31, 'ESPIRITO SANTO': 32, 'ESPÍRITO SANTO': 32,
    'RIO DE JANEIRO': 33, 'SAO PAULO': 35, 'SÃO PAULO': 35,
    'PARANA': 41, 'PARANÁ': 41, 'SANTA CATARINA': 42, 'RIO GRANDE DO SUL': 43,
    'MATO GROSSO DO SUL': 50, 'MATO GROSSO': 51, 'GOIAS': 52, 'GOIÁS': 52, 'DISTRITO FEDERAL': 53
}
IBGE_TO_SIGLA = {11:'RO', 12:'AC', 13:'AM', 14:'RR', 15:'PA', 16:'AP', 17:'TO', 21:'MA', 22:'PI', 23:'CE', 24:'RN', 25:'PB', 26:'PE', 27:'AL', 28:'SE', 29:'BA', 31:'MG', 32:'ES', 33:'RJ', 35:'SP', 41:'PR', 42:'SC', 43:'RS', 50:'MS', 51:'MT', 52:'GO', 53:'DF'}

# --- 3. ETL CORE CLASS ---
class PisaUnifiedETL:
    
    def run_2015(self):
        print("\n[PROCESS] PISA 2015 - UF Level...")
        base_path = DATA_RAW_ROOT / 'pisa_2015'
        target_file = next(base_path.glob('*STU*.sav'), None)
        if not target_file: 
            print("[ERROR] 2015 raw file not found."); return

        df, meta = pyreadstat.read_sav(str(target_file))
        df = df[df['CNT'] == 'BRA'].copy()
        labels = meta.variable_value_labels.get('STRATUM', {})
        df['STRATUM_TEXT'] = df['STRATUM'].map(labels).fillna(df['STRATUM'].astype(str))
        
        # Mapping logic
        df['IBGE_CODE'] = df['STRATUM_TEXT'].apply(lambda x: next((NAME_TO_IBGE[n] for n in sorted(NAME_TO_IBGE.keys(), key=len, reverse=True) if n in str(x).upper()), None))
        df = df.dropna(subset=['IBGE_CODE'])
        
        summary = df.groupby('IBGE_CODE')[['PV1MATH', 'PV1READ', 'PV1SCIE']].mean().reset_index()
        summary['UF'] = summary['IBGE_CODE'].map(IBGE_TO_SIGLA)
        summary['Cognitive_Global_Mean'] = summary[['PV1MATH', 'PV1READ', 'PV1SCIE']].mean(axis=1)
        
        df_final = summary.rename(columns={'PV1MATH': 'Math', 'PV1READ': 'Read', 'PV1SCIE': 'Science'}).sort_values('Cognitive_Global_Mean', ascending=False)
        
        # Final Exports
        df_final.to_csv(DATA_PROCESSED_DIR / 'pisa_2015_states.csv', index=False)
        df_final.to_excel(REPORT_DIR / 'pisa_2015_states.xlsx', index=False)
        print("[SUCCESS] Exported PISA 2015.")

    def run_2018(self):
        print("\n[PROCESS] PISA 2018 - Regional...")
        file_path = DATA_RAW_ROOT / 'pisa_2018' / 'CY07_MSU_STU_QQQ.sav'
        if not file_path.exists(): 
            print("[ERROR] 2018 raw file not found."); return

        df = pd.read_spss(str(file_path), usecols=['CNT', 'STRATUM', 'PV1MATH', 'PV1READ', 'PV1SCIE'], convert_categoricals=False)
        df = df[df['CNT'].astype(str).str.contains('BRA|76')].copy()
        
        mapping = {'01':'North', '02':'Northeast', '03':'Southeast', '04':'South', '05':'Center-West'}
        df['Region'] = df['STRATUM'].apply(lambda x: mapping.get(str(x).upper()[3:5], 'UNKNOWN'))
        df = df[df['Region'] != 'UNKNOWN']
        
        means = df.groupby('Region')[['PV1MATH', 'PV1READ', 'PV1SCIE']].mean().reset_index()
        counts = df.groupby('Region').size().reset_index(name='Student_Count')
        
        res = pd.merge(counts, means, on='Region')
        res['Cognitive_Global_Mean'] = res[['PV1MATH', 'PV1READ', 'PV1SCIE']].mean(axis=1)
        df_final = res.rename(columns={'PV1MATH': 'Math_Mean', 'PV1READ': 'Read_Mean', 'PV1SCIE': 'Science_Mean'}).round(2).sort_values('Cognitive_Global_Mean', ascending=False)
        
        # Final Exports
        df_final.to_csv(DATA_PROCESSED_DIR / 'pisa_2018_regional_summary.csv', index=False)
        df_final.to_excel(REPORT_DIR / 'pisa_2018_regional_summary.xlsx', index=False)
        print("[SUCCESS] Exported PISA 2018.")

    def run_2022(self):
        print("\n[PROCESS] PISA 2022 - Regional...")
        file_path = DATA_RAW_ROOT / 'pisa_2022' / 'CY08MSP_STU_QQQ.sav'
        if not file_path.exists(): 
            print("[ERROR] 2022 raw file not found."); return

        df = pd.read_spss(str(file_path), usecols=['CNT', 'STRATUM', 'PV1MATH', 'PV1READ', 'PV1SCIE'])
        df = df[df['CNT'].astype(str).str.contains('Brazil')].copy()
        
        def get_reg(s):
            s = str(s).upper()
            return 'North' if 'NORTE' in s else 'Northeast' if 'NORDESTE' in s else 'Southeast' if 'SUDESTE' in s else 'South' if 'SUL' in s else 'Center-West' if 'CENTRO' in s else 'UNKNOWN'
        
        df['Region'] = df['STRATUM'].apply(get_reg)
        df = df[df['Region'] != 'UNKNOWN']
        
        means = df.groupby('Region')[['PV1MATH', 'PV1READ', 'PV1SCIE']].mean().reset_index()
        counts = df.groupby('Region').size().reset_index(name='Student_Count')
        
        res = pd.merge(counts, means, on='Region')
        res['Cognitive_Global_Mean'] = res[['PV1MATH', 'PV1READ', 'PV1SCIE']].mean(axis=1)
        df_final = res.rename(columns={'PV1MATH': 'Math_Mean', 'PV1READ': 'Read_Mean', 'PV1SCIE': 'Science_Mean'}).round(2).sort_values('Cognitive_Global_Mean', ascending=False)
        
        # Final Exports
        df_final.to_csv(DATA_PROCESSED_DIR / 'pisa_2022_regional_summary.csv', index=False)
        df_final.to_excel(REPORT_DIR / 'pisa_2022_regional_summary.xlsx', index=False)
        print("[SUCCESS] Exported PISA 2022.")

# --- 4. EXECUTION FLOW ---
def main():
    print("="*60 + "\n   COGNITIVE CAPITAL - PISA ETL PRODUCTION (v8.0)\n" + "="*60)
    print("Warning: Processing large SPSS (.sav) files may take several minutes.\n")
    print("1. PISA 2015 (State/UF Level)")
    print("2. PISA 2018 (Regional Level)")
    print("3. PISA 2022 (Regional Level)")
    print("4. ALL CYCLES")
    print("Q. QUIT")
    
    choice = input("\nSelect an option (1-4 or Q): ").strip().upper()
    if choice == 'Q': sys.exit(0)
    
    etl = PisaUnifiedETL()
    start = time.perf_counter()
    
    try:
        if choice in ['1', '4']: etl.run_2015()
        if choice in ['2', '4']: etl.run_2018()
        if choice in ['3', '4']: etl.run_2022()
        print(f"\n[TIMER] Finished in: {str(timedelta(seconds=time.perf_counter() - start))}")
    except Exception as e:
        print(f"\n[ERROR] {e}")

if __name__ == "__main__":
    main()