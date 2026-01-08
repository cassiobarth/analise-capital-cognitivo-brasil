"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/cog/04_process_enem_historical.py
DESCRIPTION: Extracts ENEM 2015 and 2018 to Engineering & Analytics.
"""
import pandas as pd
import zipfile
from pathlib import Path

# --- CONFIGURATION ---
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent.parent

PROC_DIR = PROJECT_ROOT / 'data' / 'processed'
REPORT_XLSX_DIR = PROJECT_ROOT / 'reports' / 'varcog' / 'xlsx'
RAW_DIR = PROJECT_ROOT / 'data' / 'raw'

YEARS = [2015, 2018]
COLS = ['SG_UF_RESIDENCIA', 'NU_NOTA_CN', 'NU_NOTA_CH', 'NU_NOTA_LC', 'NU_NOTA_MT', 'NU_NOTA_REDACAO']

def process_enem_year(year):
    zip_path = RAW_DIR / f'microdados_enem_{year}.zip'
    print(f"[INFO] Processing ENEM {year}...")
    
    if not zip_path.exists():
        print(f"   [!] Zip not found: {zip_path}")
        return

    try:
        chunks = []
        with zipfile.ZipFile(zip_path) as z:
            fname = next((f for f in z.namelist() if 'MICRODADOS_ENEM' in f and f.endswith('.csv')), None)
            if not fname: return

            with z.open(fname) as f:
                for chunk in pd.read_csv(f, sep=';', encoding='latin1', usecols=COLS, chunksize=500000):
                    # Calcular média das 5 notas na linha
                    chunk['Mean_General'] = chunk.iloc[:, 1:].mean(axis=1)
                    # Agregar parcial
                    agg = chunk.groupby('SG_UF_RESIDENCIA')['Mean_General'].agg(['sum', 'count'])
                    chunks.append(agg)
        
        # Consolidar Chunks
        full = pd.concat(chunks).groupby(level=0).sum()
        full['Mean_General'] = full['sum'] / full['count']
        
        # Formatar
        out = full[['Mean_General']].reset_index().rename(columns={'SG_UF_RESIDENCIA': 'UF'})
        out = out.sort_values('Mean_General', ascending=False).round(2)

        # 1. Save Engineering
        proc_path = PROC_DIR / f'enem_table_{year}.csv'
        out.to_csv(proc_path, index=False, sep=';') # Mantendo padrão ; do script triennium
        print(f"   [ENG] Saved: {proc_path}")

        # 2. Save Analytics
        rep_path = REPORT_XLSX_DIR / f'enem_table_{year}.xlsx'
        out.to_excel(rep_path, index=False)

    except Exception as e:
        print(f"   [ERROR] {e}")

if __name__ == "__main__":
    for p in [PROC_DIR, REPORT_XLSX_DIR]: p.mkdir(parents=True, exist_ok=True)
    for y in YEARS: process_enem_year(y)