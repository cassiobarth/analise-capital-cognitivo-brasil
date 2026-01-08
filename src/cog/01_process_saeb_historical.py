"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/cog/01_process_saeb_historical.py
DESCRIPTION: Extracts SAEB 2015 and 2017 (High School) to Engineering & Analytics.
"""
import pandas as pd
import numpy as np
import zipfile
from pathlib import Path

# --- CONFIGURATION ---
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent.parent

PROC_DIR = PROJECT_ROOT / 'data' / 'processed'
REPORT_XLSX_DIR = PROJECT_ROOT / 'reports' / 'varcog' / 'xlsx'

RAW_DIR = PROJECT_ROOT / 'data' / 'raw'
TARGETS = {
    2015: {'file': 'microdados_saeb_2015.zip', 'cols': ['ID_UF', 'PROFICIENCIA_MT', 'PROFICIENCIA_LP']},
    2017: {'file': 'microdados_saeb_2017.zip', 'cols': ['ID_UF', 'PROFICIENCIA_MT', 'PROFICIENCIA_LP']}
}

IBGE_MAP = {11:'RO',12:'AC',13:'AM',14:'RR',15:'PA',16:'AP',17:'TO',21:'MA',22:'PI',23:'CE',24:'RN',25:'PB',26:'PE',27:'AL',28:'SE',29:'BA',31:'MG',32:'ES',33:'RJ',35:'SP',41:'PR',42:'SC',43:'RS',50:'MS',51:'MT',52:'GO',53:'DF'}

def setup():
    for p in [PROC_DIR, REPORT_XLSX_DIR]: p.mkdir(parents=True, exist_ok=True)

def process_year(year, config):
    zip_path = RAW_DIR / config['file']
    print(f"[INFO] Processing SAEB {year} from {zip_path.name}...")
    
    if not zip_path.exists():
        print(f"   [!] File not found: {zip_path}")
        return

    try:
        with zipfile.ZipFile(zip_path) as z:
            # Tenta achar arquivo de alunos 3º ano
            fname = next((f for f in z.namelist() if 'TS_ALUNO_3' in f), None)
            if not fname:
                print("   [!] CSV TS_ALUNO not found in zip.")
                return

            with z.open(fname) as f:
                df = pd.read_csv(f, sep=';', encoding='latin1', usecols=config['cols'])

        # Padronizar
        c_uf = [c for c in config['cols'] if 'UF' in c][0]
        c_mt = [c for c in config['cols'] if 'MT' in c][0]
        c_lp = [c for c in config['cols'] if 'LP' in c][0]

        df['UF'] = df[c_uf].map(IBGE_MAP)
        df['MT'] = pd.to_numeric(df[c_mt], errors='coerce')
        df['LP'] = pd.to_numeric(df[c_lp], errors='coerce')
        
        # Média
        agg = df.groupby('UF')[['MT', 'LP']].mean().reset_index()
        agg['SAEB_General'] = (agg['MT'] + agg['LP']) / 2
        
        # Formatar saída
        out = agg[['UF', 'SAEB_General', 'MT', 'LP']].round(2).sort_values('SAEB_General', ascending=False)

        # 1. Save Engineering
        proc_path = PROC_DIR / f'saeb_{year}_states.csv'
        out.to_csv(proc_path, index=False)
        print(f"   [ENG] Saved: {proc_path}")

        # 2. Save Analytics
        rep_path = REPORT_XLSX_DIR / f'saeb_{year}_states.xlsx'
        out.to_excel(rep_path, index=False)
        print(f"   [RPT] Saved: {rep_path}")

    except Exception as e:
        print(f"   [ERROR] {e}")

if __name__ == "__main__":
    setup()
    for year, cfg in TARGETS.items():
        process_year(year, cfg)