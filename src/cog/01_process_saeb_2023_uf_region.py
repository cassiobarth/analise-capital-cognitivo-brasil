"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/cog/01_process_saeb_2023.py
RESEARCHERS: Dr. José Aparecido da Silva
             Me. Cássio Dalbem Barth
DATE:        2026-01-08 (Updated)

DESCRIPTION: 
    Extracts SAEB 2023 data (School Level).
    - Maps IBGE codes to States (UF) and Macro-Regions.
    - Calculates Mean Scores (Math + Portuguese) for High School (EM).
    - EXPORTS TO ENGINEERING: data/processed/saeb_2023_states.csv
    - EXPORTS TO ANALYTICS:   reports/varcog/csv/ & xlsx/
"""
import pandas as pd
import numpy as np
import os
import zipfile
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# --- 1. CONFIGURATION ---
SEED = 42
np.random.seed(SEED)

# --- PATH CONFIGURATION ---
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent.parent

# Input
ZIP_PATH = PROJECT_ROOT / 'data' / 'raw' / 'microdados_saeb_2023.zip'

# 1. Engineering Outputs (CSV for modeling)
PROC_DIR = PROJECT_ROOT / 'data' / 'processed'
PATH_PROC_CSV = PROC_DIR / 'saeb_2023_states.csv'

# 2. Reporting Outputs (XLSX and graphs for analytics)
REPORT_XLSX_DIR = PROJECT_ROOT / 'reports' / 'varcog'/ 'xlsx'
PATH_XLSX = REPORT_XLSX_DIR / 'saeb_2023_states.xlsx'

REPORT_IMG_DIR = PROJECT_ROOT / 'reports' / 'graficos'
PATH_IMG = REPORT_IMG_DIR / 'ranking_saeb_2023.png'

# --- MAPPINGS ---
IBGE_CODE_MAP = {
    11: 'RO', 12: 'AC', 13: 'AM', 14: 'RR', 15: 'PA', 16: 'AP', 17: 'TO',
    21: 'MA', 22: 'PI', 23: 'CE', 24: 'RN', 25: 'PB', 26: 'PE', 27: 'AL', 28: 'SE', 29: 'BA',
    31: 'MG', 32: 'ES', 33: 'RJ', 35: 'SP',
    41: 'PR', 42: 'SC', 43: 'RS',
    50: 'MS', 51: 'MT', 52: 'GO', 53: 'DF'
}

UF_REGION_MAP = {
    'RO': 'North', 'AC': 'North', 'AM': 'North', 'RR': 'North', 'PA': 'North', 'AP': 'North', 'TO': 'North',
    'MA': 'Northeast', 'PI': 'Northeast', 'CE': 'Northeast', 'RN': 'Northeast', 'PB': 'Northeast', 
    'PE': 'Northeast', 'AL': 'Northeast', 'SE': 'Northeast', 'BA': 'Northeast',
    'MG': 'Southeast', 'ES': 'Southeast', 'RJ': 'Southeast', 'SP': 'Southeast',
    'PR': 'South', 'SC': 'South', 'RS': 'South',
    'MS': 'Center-West', 'MT': 'Center-West', 'GO': 'Center-West', 'DF': 'Center-West'
}

def setup_directories():
    # Cria todas as pastas necessárias antes de começar
    for p in [PROC_DIR, REPORT_XLSX_DIR, REPORT_IMG_DIR]:
        p.mkdir(parents=True, exist_ok=True)

def load_data():
    print(f"[INFO] Reading ZIP: {ZIP_PATH.name}")
    try:
        with zipfile.ZipFile(ZIP_PATH) as z:
            target = next(f for f in z.namelist() if 'TS_ESCOLA.csv' in f)
            print(f"       - Found target: {target}")
            cols = ['ID_UF', 'MEDIA_EM_LP', 'MEDIA_EM_MT']
            with z.open(target) as f:
                return pd.read_csv(f, sep=';', usecols=cols, encoding='latin1')
    except Exception as e:
        print(f"[ERROR] Failed to load data: {e}")
        return None

def process_data(df):
    print("[INFO] Processing data...")
    for col in ['MEDIA_EM_LP', 'MEDIA_EM_MT']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.dropna(subset=['MEDIA_EM_LP', 'MEDIA_EM_MT']).copy()

    df['ID_UF'] = pd.to_numeric(df['ID_UF'], errors='coerce').fillna(0).astype(int)
    df['UF'] = df['ID_UF'].map(IBGE_CODE_MAP)
    df['Region'] = df['UF'].map(UF_REGION_MAP)

    if df['Region'].isna().any():
        print(f"[WARNING] Rows unmapped to Region.")

    res = df.groupby(['Region', 'UF'], as_index=False)[['MEDIA_EM_MT', 'MEDIA_EM_LP']].mean()
    res['SAEB_General'] = (res['MEDIA_EM_MT'] + res['MEDIA_EM_LP']) / 2
    
    return res.round(2).sort_values('SAEB_General', ascending=False)[['Region', 'UF', 'MEDIA_EM_MT', 'MEDIA_EM_LP', 'SAEB_General']]

def generate_chart(df):
    print("[INFO] Generating Chart...")
    plt.figure(figsize=(12, 8))
    sns.set_theme(style="whitegrid")
    
    sns.barplot(
        x='SAEB_General', y='UF', data=df, 
        palette='viridis', hue='Region', dodge=False
    )
    
    plt.title('SAEB 2023 Ranking - High School', fontsize=14)
    plt.xlabel('Mean Score')
    plt.tight_layout()
    plt.savefig(PATH_IMG, dpi=300)
    plt.close()
    print(f"       - Image saved: {PATH_IMG}")

if __name__ == "__main__":
    setup_directories()
    
    if ZIP_PATH.exists():
        df_raw = load_data()
        
        if df_raw is not None:
            df_final = process_data(df_raw)
            
            # 1. Save to ENGINEERING (Processed - CSV)
            df_final.to_csv(PATH_PROC_CSV, index=False)
            print(f"[ENGINEERING] CSV Saved: {PATH_PROC_CSV}")
            
            # 2. Save to ANALYTICS (Report - XLSX)
            df_final.to_excel(PATH_XLSX, index=False)
            print(f"[ANALYTICS] Excel Saved: {PATH_XLSX}")
            
            generate_chart(df_final)
    else:
        print(f"[ERROR] File not found: {ZIP_PATH}")