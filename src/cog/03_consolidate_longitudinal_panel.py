"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/cog/03_consolidate_longitudinal_panel.py
DESCRIPTION: Merges PROCESSED PISA, SAEB, and ENEM files into the Master Panel.
"""
import pandas as pd
from pathlib import Path

# --- CONFIGURATION ---
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent.parent
PROC_DIR = PROJECT_ROOT / 'data' / 'processed'
REPORT_XLSX_DIR = PROJECT_ROOT / 'reports' / 'varcog' / 'xlsx'

# Definição das Ondas e onde buscar os arquivos PROCESSADOS
WAVES = {
    2015: {
        'pisa': PROC_DIR / 'pisa_2015_states.csv',
        'saeb': PROC_DIR / 'saeb_2015_states.csv',
        'enem': PROC_DIR / 'enem_table_2015.csv'
    },
    2018: {
        'pisa': PROC_DIR / 'pisa_2018_brazil_states.csv', # Verifique o nome exato gerado pelo script 02
        'saeb': PROC_DIR / 'saeb_2017_states.csv',        # Proxy temporal
        'enem': PROC_DIR / 'enem_table_2018.csv'
    },
    2022: {
        'pisa': PROC_DIR / 'pisa_2022_calculado_oficial.csv',
        'saeb': PROC_DIR / 'saeb_2023_states.csv',        # Proxy temporal
        'enem': PROC_DIR / 'enem_table_2022.csv'
    }
}

def load_processed(path, type_):
    if not path.exists(): return None
    
    # Tratamento para CSVs com separadores diferentes
    try:
        df = pd.read_csv(path, sep=';') if 'enem' in str(path) else pd.read_csv(path)
        # Fallback se a leitura falhar (ex: era virgula mas tentou ponto e virgula)
        if df.shape[1] < 2: 
            df = pd.read_csv(path, sep=',' if 'enem' in str(path) else ';')
    except:
        return None

    # Padronização de Colunas
    df.columns = [c.upper() for c in df.columns] # Normalizar caixa alta
    
    # Renomear para UF e SCORE
    cols = df.columns
    uf_col = next((c for c in cols if 'UF' in c), None)
    
    # Identificar coluna de nota
    score_col = None
    if type_ == 'pisa':
        score_col = next((c for c in cols if 'GLOBAL' in c or 'MEAN' in c), None)
    elif type_ == 'saeb':
        score_col = next((c for c in cols if 'GENERAL' in c or 'SCORE' in c), None)
    elif type_ == 'enem':
        score_col = next((c for c in cols if 'GENERAL' in c or 'MEAN' in c), None)

    if uf_col and score_col:
        return df[[uf_col, score_col]].rename(columns={uf_col: 'UF', score_col: f'{type_.upper()}_SCORE'})
    
    return None

if __name__ == "__main__":
    print("[INFO] Building Master Panel from Processed Data...")
    frames = []

    for year, files in WAVES.items():
        print(f"   > Assembling Wave {year}...")
        
        # Carregar
        pisa = load_processed(files['pisa'], 'pisa')
        saeb = load_processed(files['saeb'], 'saeb')
        enem = load_processed(files['enem'], 'enem')
        
        # Merge
        parts = [d for d in [pisa, saeb, enem] if d is not None]
        if parts:
            wave_df = parts[0]
            for p in parts[1:]:
                wave_df = pd.merge(wave_df, p, on='UF', how='outer')
            
            wave_df['WAVE'] = year
            frames.append(wave_df)

    if frames:
        master = pd.concat(frames, ignore_index=True)
        # Reordenar
        cols = ['WAVE', 'UF'] + [c for c in master.columns if c not in ['WAVE', 'UF']]
        master = master[cols]

        # Salvar
        master.to_csv(PROC_DIR / 'panel_longitudinal_waves.csv', index=False)
        master.to_excel(REPORT_XLSX_DIR / 'tabela_analitica_completa.xlsx', index=False)
        print(f"[SUCCESS] Master Panel Created with {len(master)} rows.")
    else:
        print("[FAIL] No processed files found. Run extractors (01, 02, 04) first.")