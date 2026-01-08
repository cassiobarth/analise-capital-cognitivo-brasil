"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/cog/02_process_pisa_2015_uf_region.py
RESEARCHERS: Dr. Jose Aparecido da Silva
             Me. Cassio Dalbem Barth
DATE:        2026-01-08 (Fix v2.4: Path Correction)

DESCRIPTION:
    Extracts PISA 2015 Student Data (SPSS format) for Brazil.
    
    IMPROVEMENTS:
    1. Label Decoding: Applies SPSS value labels to map 'STRATUM' codes to text.
    2. Geocoding: Maps State Names (e.g. "São Paulo") to IBGE codes.
    3. Reporting: 
       - CSV (Processed Data) -> data/processed/
       - XLSX (Analytics Report) -> reports/varcog/xlsx/

INPUT:
    - data/raw/Pisa/pisa_2015/*STU*.sav

OUTPUT:
    - data/processed/pisa_2015_states.csv
    - reports/varcog/xlsx/pisa_2015_states.xlsx
"""

import pandas as pd
import os
import sys
import pyreadstat
import numpy as np
import re

# --- 1. SAFEGUARD IMPORT PROTOCOL ---
script_dir = os.path.dirname(os.path.abspath(__file__))
lib_path = os.path.join(script_dir, 'lib')
if lib_path not in sys.path: sys.path.append(lib_path)

try:
    from safeguard import DataGuard
except ImportError:
    DataGuard = None

# --- 2. CONFIGURATION & PATHS ---
BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_RAW = os.path.join(BASE_PATH, 'data', 'raw', 'Pisa', 'pisa_2015')

# CORRECTED OUTPUT DIRECTORIES
# CSVs go to the main data pipeline folder
CSV_DIR = os.path.join(BASE_PATH, 'data', 'processed')
# Excel reports go to the analytics folder
XLSX_DIR = os.path.join(BASE_PATH, 'reports', 'varcog', 'xlsx')

os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(XLSX_DIR, exist_ok=True)

# --- 3. MAPPING DICTIONARIES ---
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

REGIONAL_MAP = {
    'N': [11, 12, 13, 14, 15, 16, 17],
    'NE': [21, 22, 23, 24, 25, 26, 27, 28, 29],
    'SE': [31, 32, 33, 35],
    'S': [41, 42, 43],
    'CO': [50, 51, 52, 53]
}
IBGE_TO_REGION = {code: reg for reg, codes in REGIONAL_MAP.items() for code in codes}

IBGE_TO_SIGLA = {
    11:'RO', 12:'AC', 13:'AM', 14:'RR', 15:'PA', 16:'AP', 17:'TO',
    21:'MA', 22:'PI', 23:'CE', 24:'RN', 25:'PB', 26:'PE', 27:'AL', 28:'SE', 29:'BA',
    31:'MG', 32:'ES', 33:'RJ', 35:'SP', 41:'PR', 42:'SC', 43:'RS',
    50:'MS', 51:'MT', 52:'GO', 53:'DF'
}

def resolve_ibge_from_text(text_label):
    if not isinstance(text_label, str): return None
    text_upper = text_label.upper()
    sorted_names = sorted(NAME_TO_IBGE.keys(), key=len, reverse=True)
    for name in sorted_names:
        if name in text_upper: return NAME_TO_IBGE[name]
    return None

def process_pisa_2015():
    print("="*60)
    print("[START] PISA 2015 Extraction (v2.3 Excel+CSV)")
    print("="*60)

    if not os.path.exists(DATA_RAW):
        print(f"[ERROR] Directory missing: {DATA_RAW}")
        return

    sav_files = [f for f in os.listdir(DATA_RAW) if 'STU' in f and f.endswith('.sav')]
    if not sav_files:
        print("[CRITICAL] No Student (STU) file found.")
        return

    target_file = os.path.join(DATA_RAW, sav_files[0])
    print(f"[FILE] Reading: {sav_files[0]}")

    try:
        # Metadata Scan
        _, meta = pyreadstat.read_sav(target_file, metadataonly=True)
        
        candidates = ['STRATUM', 'REGION', 'CNT', 'ST004D01T']
        region_col = next((c for c in candidates if c in meta.column_names), None)
        scores = [c for c in meta.column_names if c.startswith('PV1') and any(x in c for x in ['MATH', 'READ', 'SCIE'])]
        
        if not region_col:
            print("[CRITICAL] No region column found.")
            return

        use_cols = list(set([region_col] + scores))
        if 'CNT' in meta.column_names: use_cols.append('CNT')

        # Data Load
        print(f"[INFO] Loading {len(use_cols)} columns...")
        df, meta = pyreadstat.read_sav(target_file, usecols=use_cols)
        
        if 'CNT' in df.columns:
            df = df[df['CNT'] == 'BRA'].copy()

        # Apply Labels
        if region_col in meta.variable_value_labels:
            print(f"[INFO] Decoding '{region_col}' labels...")
            labels = meta.variable_value_labels[region_col]
            df['STRATUM_TEXT'] = df[region_col].map(labels).fillna(df[region_col].astype(str))
        else:
            df['STRATUM_TEXT'] = df[region_col].astype(str)

        # Geocoding
        df['IBGE_CODE'] = df['STRATUM_TEXT'].apply(resolve_ibge_from_text)
        valid_rows = df['IBGE_CODE'].notnull().sum()
        print(f"[STATS] Mapped Rows: {valid_rows}/{len(df)}")
        
        if valid_rows == 0:
            print("[CRITICAL] Mapping failed. Aborting.")
            return

        # Cleaning
        df = df.dropna(subset=['IBGE_CODE'])
        cols_math = [c for c in df.columns if 'MATH' in c]
        cols_read = [c for c in df.columns if 'READ' in c]
        cols_scie = [c for c in df.columns if 'SCIE' in c]
        
        if cols_math: df['Math'] = df[cols_math[0]]
        if cols_read: df['Read'] = df[cols_read[0]]
        if cols_scie: df['Science'] = df[cols_scie[0]]

        # Aggregation
        summary = df.groupby('IBGE_CODE')[['Math', 'Read', 'Science']].mean().reset_index()
        summary['UF'] = summary['IBGE_CODE'].map(IBGE_TO_SIGLA)
        summary['Region'] = summary['IBGE_CODE'].map(IBGE_TO_REGION)
        summary['Cognitive_Global_Mean'] = summary[['Math', 'Read', 'Science']].mean(axis=1)
        summary = summary.sort_values(by='Cognitive_Global_Mean', ascending=False)  # <--- ORDENAÇÃO
        summary = summary[['Region', 'UF', 'Math', 'Read', 'Science', 'Cognitive_Global_Mean']]

        # SafeGuard
        if DataGuard:
            print("[AUDIT] Verifying Consistency...")
            guard = DataGuard(summary, "PISA 2015")
            guard.check_historical_consistency('Cognitive_Global_Mean', 'UF')
            guard.validate(strict=True)

        # Save Outputs
        out_csv = os.path.join(CSV_DIR, 'pisa_2015_states.csv')
        out_xlsx = os.path.join(XLSX_DIR, 'pisa_2015_states.xlsx')
        
        summary.to_csv(out_csv, index=False)
        summary.to_excel(out_xlsx, index=False)
        
        print(f"[SUCCESS] Reports Generated:")
        print(f"          CSV:  {out_csv}")
        print(f"          XLSX: {out_xlsx}")

    except Exception as e:
        print(f"[CRITICAL FAILURE] {e}")
        sys.exit(1)

if __name__ == "__main__":
    process_pisa_2015()