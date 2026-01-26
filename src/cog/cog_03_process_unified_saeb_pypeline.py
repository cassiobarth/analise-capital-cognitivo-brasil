"""
================================================================================
PROJECT:        COGNITIVE CAPITAL ANALYSIS - BRAZIL
SCRIPT:         src/cog/cog_02_process_unified_saeb_pypeline.py
AUTHOR:         Me. Cássio Dalbem Barth
VERSION:        14.6 (Production - Student Count + XLSX Support)
DATE:           2026-01-19
--------------------------------------------------------------------------------
PRINCIPAL INVESTIGATOR:  Dr. José Aparecido da Silva
SOURCE:                  INEP Microdata (SAEB / Prova Brasil)
================================================================================

ABSTRACT:
    Unified ETL pipeline for SAEB microdata processing.
    - Extracts School Averages (Proficiency) and Student Volume (N).
    - Uses brute-force column detection for robustness across years.
    - Outputs standardized CSV matrices (Data) and XLSX (Reports).

RASTREABILITY SETTINGS:
    - INPUT_ROOT:  data/raw/saeb/
    - OUTPUT_CSV:  data/processed/saeb_table_[year]_[grade].csv
    - OUTPUT_XLSX: reports/varcog/xlsx/saeb_table_[year]_[grade].xlsx

DEPENDENCIES:
    pandas, numpy, zipfile, logging, os, openpyxl
================================================================================
"""
import pandas as pd
import numpy as np
import os
import zipfile
import sys
import logging
import time

# --- WINDOWS TIMEOUT INPUT ---
try:
    import msvcrt
    def input_timeout(prompt, timeout=5, default=''):
        print(f"{prompt} [Auto em {timeout}s]: ", end='', flush=True)
        start_time = time.time()
        input_chars = []
        while True:
            if msvcrt.kbhit():
                char = msvcrt.getwche()
                if char == '\r': 
                    print()
                    return "".join(input_chars)
                input_chars.append(char)
                return "".join(input_chars) + input() 
            if (time.time() - start_time) > timeout:
                print(f"\n[TIMEOUT] Padrão assumido.")
                return default
            time.sleep(0.05)
except ImportError:
    def input_timeout(prompt, timeout=5, default=''):
        return input(f"{prompt} [Enter para Padrão]: ")

# --- GLOBAL CONFIG ---
BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_RAW = os.path.join(BASE_PATH, 'data', 'raw', 'saeb')
DATA_PROCESSED = os.path.join(BASE_PATH, 'data', 'processed')
REPORT_XLSX = os.path.join(BASE_PATH, 'reports', 'varcog', 'xlsx') 
LOG_DIR = os.path.join(BASE_PATH, 'logs')

# Criação garantida dos diretórios
for p in [DATA_RAW, DATA_PROCESSED, LOG_DIR, REPORT_XLSX]: 
    os.makedirs(p, exist_ok=True)

IBGE_TO_SIGLA = {
    11:'RO', 12:'AC', 13:'AM', 14:'RR', 15:'PA', 16:'AP', 17:'TO',
    21:'MA', 22:'PI', 23:'CE', 24:'RN', 25:'PB', 26:'PE', 27:'AL', 28:'SE', 29:'BA',
    31:'MG', 32:'ES', 33:'RJ', 35:'SP', 41:'PR', 42:'SC', 43:'RS',
    50:'MS', 51:'MT', 52:'GO', 53:'DF'
}

class SaebPipeline:
    def __init__(self, year, file_path, filter_network):
        self.year = year
        self.file_path = file_path
        self.filter_network = filter_network 

    def detect_separator(self, f_handle):
        try:
            line = f_handle.readline().decode('latin1')
            f_handle.seek(0)
            return ';' if line.count(';') > line.count(',') else ','
        except:
            f_handle.seek(0)
            return ';'

    def find_col_flexible(self, header, candidates, substring_fallback=None):
        header_upper = {h.upper(): h for h in header}
        # 1. Exact Match (Case Insensitive)
        for cand in candidates:
            if cand.upper() in header_upper: return header_upper[cand.upper()]
        # 2. Substring Match
        if substring_fallback:
            for h in header:
                if substring_fallback.upper() in h.upper(): return h
        return None

    def get_quantity_column(self, header, grade):
        """
        [NEW] Robust brute-force search for Student Count column.
        """
        header_upper = {h.upper(): h for h in header}
        
        candidates = [
            f"NU_PRESENTES_{grade}",      # Padrão 2015/2017
            f"NU_PRESENTES_{grade}_LP",   # Padrão 2019+
            f"QTD_ALUNOS_{grade}",        
            f"N_ALUNOS_{grade}",
            "NU_PRESENTES",               
            # Variações específicas para 3EM (as vezes 'EM')
            f"NU_PRESENTES_EM" if grade == '3EM' else "X_IGNORE",
            f"NU_PRESENTES_EM_LP" if grade == '3EM' else "X_IGNORE"
        ]
        
        for cand in candidates:
            if cand in header_upper:
                return header_upper[cand]
        return None

    def find_grade_columns(self, header, grade):
        # Busca Notas
        lp = next((h for h in header if ('MEDIA' in h.upper() or 'PROFICIENCIA' in h.upper()) and grade in h.upper() and ('LP' in h.upper() or 'LINGUA' in h.upper())), None)
        mt = next((h for h in header if ('MEDIA' in h.upper() or 'PROFICIENCIA' in h.upper()) and grade in h.upper() and ('MT' in h.upper() or 'MAT' in h.upper())), None)
        
        # [NEW] Busca Quantidade (N)
        qty = self.get_quantity_column(header, grade)

        # Fallback para 3EM (tag 'EM')
        if not (lp and mt) and grade == '3EM':
            lp = next((h for h in header if 'MEDIA' in h.upper() and '_EM_' in h.upper() and 'LP' in h.upper()), None)
            mt = next((h for h in header if 'MEDIA' in h.upper() and '_EM_' in h.upper() and 'MT' in h.upper()), None)
            if not qty: qty = self.get_quantity_column(header, "EM")

        return lp, mt, qty

    def get_region(self, uf):
        regions = {
            'Norte': ['AM', 'RR', 'AP', 'PA', 'TO', 'RO', 'AC'],
            'Nordeste': ['MA', 'PI', 'CE', 'RN', 'PB', 'PE', 'AL', 'SE', 'BA'],
            'Centro-Oeste': ['MT', 'MS', 'GO', 'DF'],
            'Sudeste': ['SP', 'RJ', 'ES', 'MG'],
            'Sul': ['PR', 'RS', 'SC']
        }
        for reg, ufs in regions.items():
            if uf in ufs: return reg
        return 'Unknown'

    def process(self):
        logging.basicConfig(filename=os.path.join(LOG_DIR, f'saeb_{self.year}.log'), level=logging.INFO, force=True)
        print(f"\n[INFO] Processing SAEB {self.year}...")
        
        try:
            with zipfile.ZipFile(self.file_path, 'r') as z:
                target = next((f for f in z.namelist() if 'TS_ESCOLA' in f and f.endswith('.csv')), None)
                if not target:
                    print(f"   [ERROR] TS_ESCOLA not found in {self.year}")
                    return

                with z.open(target) as f:
                    sep = self.detect_separator(f)
                    header = pd.read_csv(f, sep=sep, encoding='latin1', nrows=0).columns.tolist()
                    
                    # 1. Identificadores Básicos
                    col_adm = self.find_col_flexible(header, ['ID_DEPENDENCIA_ADM', 'IN_PUBLICA', 'ID_REDE', 'TP_DEPENDENCIA'], substring_fallback='DEPENDENCIA')
                    col_uf = self.find_col_flexible(header, ['ID_UF', 'CO_UF', 'UF', 'SG_UF'])

                    # Loop por séries
                    for grade in ['9EF', '3EM']:
                        c_lp, c_mt, c_qty = self.find_grade_columns(header, grade)
                        
                        if not (c_lp and c_mt): continue 

                        # Load Data
                        cols = [col_uf, col_adm, c_lp, c_mt, c_qty]
                        cols = [c for c in cols if c]
                        
                        f.seek(0)
                        df = pd.read_csv(f, sep=sep, encoding='latin1', usecols=cols)

                        # Tratamento Rede
                        if col_adm:
                            df['TEMP_ADM'] = pd.to_numeric(df[col_adm], errors='coerce')
                            # 4 = Privada, Resto = Pública
                            df['Is_Public'] = df['TEMP_ADM'].apply(lambda x: 0 if x == 4 else 1)
                        else:
                            df['Is_Public'] = 1

                        # Filtro
                        if self.filter_network == 'PUBLIC': df = df[df['Is_Public'] == 1]
                        elif self.filter_network == 'PRIVATE': df = df[df['Is_Public'] == 0]

                        # Padronizar UF
                        if pd.api.types.is_numeric_dtype(df[col_uf]): df['UF'] = df[col_uf].map(IBGE_TO_SIGLA)
                        else: df['UF'] = df[col_uf]

                        # Numéricos
                        for c in [c_lp, c_mt]:
                            df[c] = pd.to_numeric(df[c].astype(str).str.replace(',', '.', regex=False), errors='coerce')
                        
                        # [NEW] Tratamento Quantidade de Alunos
                        if c_qty:
                            df[c_qty] = pd.to_numeric(df[c_qty], errors='coerce').fillna(0)
                        else:
                            df['DUMMY_QTY'] = 0

                        # Limpeza (apenas quem tem nota)
                        sub = df.dropna(subset=[c_lp, c_mt]).copy()
                        if sub.empty: continue

                        # --- AGREGAÇÃO FINAL (Mean para notas, Sum para alunos) ---
                        qty_col_name = c_qty if c_qty else 'DUMMY_QTY'
                        
                        agg_rules = {
                            c_lp: 'mean',
                            c_mt: 'mean',
                            qty_col_name: 'sum',
                            'Is_Public': 'mean'
                        }
                        
                        grouped = sub.groupby('UF').agg(agg_rules).reset_index()
                        
                        # Renomear e Calcular
                        grouped.columns = ['UF', 'Language_Mean', 'Math_Mean', 'N_Students', 'Public_Share']
                        grouped['SAEB_General'] = (grouped['Language_Mean'] + grouped['Math_Mean']) / 2
                        grouped['Grade'] = grade
                        grouped['Year'] = self.year
                        grouped['Region'] = grouped['UF'].map(lambda x: self.get_region(x))

                        final_cols = ['Region', 'UF', 'Year', 'Grade', 'SAEB_General', 'Math_Mean', 'Language_Mean', 'N_Students']
                        
                        # 1. Salvar CSV (Dados Processados)
                        fname_csv = f"saeb_table_{self.year}_{grade}.csv"
                        save_path_csv = os.path.join(DATA_PROCESSED, fname_csv)
                        grouped[final_cols].sort_values('SAEB_General', ascending=False).to_csv(save_path_csv, index=False)
                        
                        # 2. Salvar XLSX (Reports) - AQUI ESTAVA O ERRO
                        fname_xlsx = f"saeb_table_{self.year}_{grade}.xlsx"
                        save_path_xlsx = os.path.join(REPORT_XLSX, fname_xlsx)
                        grouped[final_cols].sort_values('SAEB_General', ascending=False).to_excel(save_path_xlsx, index=False)
                        
                        print(f"   -> Generated: {fname_csv} (CSV) & {fname_xlsx} (XLSX) | Alunos: {int(grouped['N_Students'].sum())}")

        except Exception as e:
            print(f"[ERROR] {e}")
            logging.error(str(e))
            import traceback
            traceback.print_exc()

def main():
    os.system('cls' if os.name == 'nt' else 'clear')
    print("=== SAEB DATA EXTRACTOR v14.6 ===")
    print(f"Raw Dir: {DATA_RAW}")

    # 1. Years
    raw = input_timeout(">> Anos (ex: 2019, 2023)", timeout=5, default="2015, 2017, 2019, 2021, 2023")
    try:
        years = [int(y.strip()) for y in raw.split(',')]
    except:
        years = [2015, 2017, 2023]

    # 2. Filter
    opt = input_timeout("\n>> Filtro (1=All, 2=Pub, 3=Priv)", timeout=5, default="1")
    filter_map = {'2': 'PUBLIC', '3': 'PRIVATE'}
    selected_filter = filter_map.get(opt.strip(), 'ALL')

    print(f"\n[CONFIG] Anos: {years} | Filtro: {selected_filter}")
    print("-" * 60)

    for y in years:
        # Busca flexivel do arquivo zip
        possible_names = [f"microdados_saeb_{y}.zip", f"TS_ESCOLA_{y}.zip"]
        final_path = None
        
        for pname in possible_names:
            p = os.path.join(DATA_RAW, pname)
            if os.path.exists(p):
                final_path = p
                break
        
        if final_path:
            SaebPipeline(y, final_path, selected_filter).process()
        else:
            print(f"   [SKIP] Arquivo não encontrado para {y} em {DATA_RAW}")

    print("\n[DONE] Extração concluída.")

if __name__ == "__main__":
    main()