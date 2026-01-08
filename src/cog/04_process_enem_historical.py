"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/cog/04_process_enem_historical.py
DESCRIPTION: Extracts ENEM 2015 and 2018 with robust column detection.
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

# Possíveis nomes para as notas
POSSIBLE_SCORES = ['NU_NOTA_CN', 'NU_NOTA_CH', 'NU_NOTA_LC', 'NU_NOTA_MT', 'NU_NOTA_REDACAO']

def find_col(options, header):
    for opt in options:
        if opt in header: return opt
    return None

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
                # 1. Detectar colunas
                header = pd.read_csv(f, sep=';', encoding='latin1', nrows=0).columns.tolist()
                
                # Fallback para UF (2015 pode usar UF_RESIDENCIA, 2018 SG_UF_RESIDENCIA)
                col_uf = find_col(['SG_UF_RESIDENCIA', 'UF_RESIDENCIA'], header)
                
                if not col_uf:
                    print(f"   [ERROR] UF column not found in {year}. Header sample: {header[:5]}")
                    return
                
                # Montar lista de uso
                use_cols = [col_uf] + POSSIBLE_SCORES
                
                # 2. Processar em Chunks
                f.seek(0)
                for chunk in pd.read_csv(f, sep=';', encoding='latin1', usecols=use_cols, chunksize=500000):
                    # Calcular média
                    chunk['Mean_General'] = chunk[POSSIBLE_SCORES].mean(axis=1)
                    # Agregar
                    chunks.append(chunk.groupby(col_uf)['Mean_General'].agg(['sum', 'count']))
        
        # Consolidar
        full = pd.concat(chunks).groupby(level=0).sum()
        full['Mean_General'] = full['sum'] / full['count']
        
        out = full[['Mean_General']].reset_index().rename(columns={col_uf: 'UF'})
        out = out.sort_values('Mean_General', ascending=False).round(2)

        # Salvar
        out.to_csv(PROC_DIR / f'enem_table_{year}.csv', index=False, sep=';')
        out.to_excel(REPORT_XLSX_DIR / f'enem_table_{year}.xlsx', index=False)
        print(f"   [SUCCESS] Saved enem_table_{year}.csv")

    except Exception as e:
        print(f"   [ERROR] {e}")

if __name__ == "__main__":
    for p in [PROC_DIR, REPORT_XLSX_DIR]: p.mkdir(parents=True, exist_ok=True)
    for y in YEARS: process_enem_year(y)