"""
================================================================================
PROJECT:        COGNITIVE CAPITAL ANALYSIS - BRAZIL
SCRIPT:         src/cog/enem_unified_pipeline.py
VERSION:        3.6 (Production - Path Fix & Robust Reader)
DATE:           2026-01-15
--------------------------------------------------------------------------------
PRINCIPAL INVESTIGATOR:  Dr. José Aparecido da Silva
LEAD DATA SCIENTIST:     Me. Cássio Dalbem Barth
SOURCE:                  INEP Microdata (Instituto Nacional de Estudos e Pesquisas)
================================================================================

ABSTRACT:
    Unified ETL pipeline for processing INEP ENEM student-level microdata. 
    Implements a hybrid filtering strategy (Strict vs. Proxy) to isolate high 
    school seniors (3EM).
    
    v3.6 CHANGE LOG:
    - Input Path: Updated to 'data/raw/enem'.
    - Stability: Implemented "Two-Pass" reading strategy (Scan -> Close -> Process)
      to permanently fix the 'Usecols do not match' zipfile cursor error.

METHODOLOGY NOTES (CRITICAL):
    1. STRICT FILTER (2015-2023): 'TP_ST_CONCLUSAO' == 2.
    2. PROXY FILTER (2024+): 'CO_ESCOLA' is NOT NULL.
    3. METRIC: State-level arithmetic mean of available cognitive domains.

RASTREABILITY SETTINGS:
    - INPUT_ROOT:  data/raw/enem/
    - OUTPUT_CSV:  data/processed/enem_table_[year]_[filter].csv
    - OUTPUT_XLSX: reports/varcog/xlsx/enem_table_[year]_[filter].xlsx

DEPENDENCIES:
    pandas, numpy, zipfile, logging, openpyxl, msvcrt (Windows)
================================================================================
"""

import pandas as pd
import numpy as np
import os
import zipfile
import logging
import time
import warnings

# Suppress DtypeWarnings for cleaner console output
warnings.filterwarnings("ignore")

# --- WINDOWS TIMEOUT INPUT UTILITY ---
try:
    import msvcrt
    def input_timeout(prompt, timeout=10, default=''):
        print(f"{prompt} [Automatic in {timeout}s]: ", end='', flush=True)
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
                print(f"\n[TIMEOUT] Executing default selection: {default}")
                return default
            time.sleep(0.05)
except ImportError:
    def input_timeout(prompt, timeout=10, default=''):
        return input(f"{prompt} [Enter for Default]: ")

# --- GLOBAL CONFIG & DIRECTORY STRUCTURE ---
BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# [FIX v3.6] Pointing specifically to the 'enem' subfolder
DATA_RAW = os.path.join(BASE_PATH, 'data', 'raw', 'enem') 

DATA_PROCESSED = os.path.join(BASE_PATH, 'data', 'processed')
REPORT_XLSX = os.path.join(BASE_PATH, 'reports', 'varcog', 'xlsx')
LOG_DIR = os.path.join(BASE_PATH, 'logs')

for p in [DATA_RAW, DATA_PROCESSED, REPORT_XLSX, LOG_DIR]:
    os.makedirs(p, exist_ok=True)

# --- TARGET COLUMN MAPPING ---
TARGET_COLS = {
    'UF': ['SG_UF_PROVA', 'UF_PROVA', 'SG_UF_ESC'], 
    'SCHOOL_ID': ['CO_ESCOLA'], 
    'STATUS': ['TP_ST_CONCLUSAO'], 
    'Natural_Sciences': ['NU_NOTA_CN'],
    'Humanities': ['NU_NOTA_CH'],
    'Language': ['NU_NOTA_LC'],
    'Math': ['NU_NOTA_MT'],
    'Essay': ['NU_NOTA_REDACAO'],
    'Foreign_Lang_Select': ['TP_LINGUA'] # 0=English, 1=Spanish
}

UF_REGION_MAP = {
    'RO': 'North', 'AC': 'North', 'AM': 'North', 'RR': 'North', 'PA': 'North', 'AP': 'North', 'TO': 'North',
    'MA': 'Northeast', 'PI': 'Northeast', 'CE': 'Northeast', 'RN': 'Northeast', 'PB': 'Northeast', 
    'PE': 'Northeast', 'AL': 'Northeast', 'SE': 'Northeast', 'BA': 'Northeast',
    'MG': 'Southeast', 'ES': 'Southeast', 'RJ': 'Southeast', 'SP': 'Southeast',
    'PR': 'South', 'SC': 'South', 'RS': 'South',
    'MS': 'Center-West', 'MT': 'Center-West', 'GO': 'Center-West', 'DF': 'Center-West'
}

class EnemPipeline:
    def __init__(self, year, file_path):
        self.year = year
        self.file_path = file_path
        self.logger = self._setup_logger()

    def _setup_logger(self):
        log_file = os.path.join(LOG_DIR, f'enem_{self.year}.log')
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        logging.basicConfig(filename=log_file, level=logging.INFO, 
                            format='%(asctime)s | %(levelname)s | %(message)s', force=True)
        return logging.getLogger()

    def get_largest_csv(self, z):
        csv_files = [f for f in z.namelist() if f.lower().endswith('.csv')]
        return sorted(csv_files, key=lambda x: z.getinfo(x).file_size, reverse=True)[0] if csv_files else None

    def find_col_flexible(self, header, candidates):
        header_upper = {h.upper(): h for h in header}
        for cand in candidates:
            if cand.upper() in header_upper:
                return header_upper[cand.upper()]
        return None

    def process(self):
        print(f"\n[START] Processing ENEM {self.year}...")
        self.logger.info(f"Pipeline started for Year {self.year}. Source: {self.file_path}")
        
        try:
            target_filename = None
            col_map = {}
            sep = ';'
            filter_mode = 'ALL_DATA'
            
            # --- PASS 1: DETECTION (Open, Scan, Close) ---
            # This pass strictly identifies the CSV structure and closes the zip cleanly.
            with zipfile.ZipFile(self.file_path, 'r') as z:
                target_filename = self.get_largest_csv(z)
                if not target_filename:
                    print("   [ERROR] No CSV found in zip archive.")
                    return

                with z.open(target_filename) as f:
                    first_line = f.readline().decode('latin-1')
                    sep = ';' if first_line.count(';') > first_line.count(',') else ','
                    f.seek(0)
                    
                    header = pd.read_csv(f, sep=sep, encoding='latin-1', nrows=0).columns.tolist()
                    
                    for k, v in TARGET_COLS.items():
                        found = self.find_col_flexible(header, v)
                        if found:
                            col_map[found] = k
                    
                    # Methodology Decision
                    has_status = 'STATUS' in col_map.values()
                    has_school = 'SCHOOL_ID' in col_map.values()
                    
                    if has_status:
                        filter_mode = 'STRICT_3EM'
                        msg = "STRICT (TP_ST_CONCLUSAO == 2)"
                    elif has_school:
                        filter_mode = 'PROXY_3EM'
                        msg = "PROXY (Active School ID Link)"
                    else:
                        filter_mode = 'ALL_DATA'
                        msg = "Fallback (All Data)"
                    
                    print(f"   [CONFIG] Methodology: {msg}")
                    self.logger.info(f"Methodology: {msg}")

            # --- PASS 2: PROCESSING (Open Fresh) ---
            # [FIX] Re-opening zip creates a fresh file pointer, solving the 'Usecols' crash.
            with zipfile.ZipFile(self.file_path, 'r') as z:
                with z.open(target_filename) as f:
                    chunk_size = 300000
                    
                    # Only request columns we CONFIRMED exist in Pass 1
                    cols_to_load = list(col_map.keys())
                    
                    reader = pd.read_csv(f, sep=sep, encoding='latin-1', usecols=cols_to_load, chunksize=chunk_size)
                    
                    agg_storage = []
                    score_cols = ['Natural_Sciences', 'Humanities', 'Language', 'Math', 'Essay']
                    
                    kept_rows = 0
                    
                    for chunk in reader:
                        chunk = chunk.rename(columns=col_map)
                        
                        # Apply Filters
                        if filter_mode == 'STRICT_3EM':
                            chunk = chunk[chunk['STATUS'] == 2].copy()
                        elif filter_mode == 'PROXY_3EM':
                            chunk = chunk[chunk['SCHOOL_ID'].notna()].copy()
                        
                        if chunk.empty: continue
                        kept_rows += len(chunk)

                        # Clean Scores
                        valid_scores = [c for c in score_cols if c in chunk.columns]
                        chunk[valid_scores] = chunk[valid_scores].apply(pd.to_numeric, errors='coerce')
                        chunk['Mean_General'] = chunk[valid_scores].mean(axis=1)

                        # Prepare Aggregation
                        group_cols = valid_scores + ['Mean_General']
                        
                        # Handle English (IND-03)
                        if 'Foreign_Lang_Select' in chunk.columns:
                            # 0 = English, 1 = Spanish (usually)
                            chunk['English_Score'] = np.where(chunk['Foreign_Lang_Select'] == 0, chunk['Language'], np.nan)
                            group_cols.append('English_Score')

                        agg_chunk = chunk.groupby('UF')[group_cols].agg(['sum', 'count'])
                        agg_storage.append(agg_chunk)

            # --- CONSOLIDATION ---
            if not agg_storage:
                print("   [WARN] No data passed the filters.")
                return

            full_agg = pd.concat(agg_storage).groupby(level=0).sum()
            final_df = pd.DataFrame(index=full_agg.index)
            
            for col in full_agg.columns.levels[0]:
                final_df[col] = full_agg[(col, 'sum')] / full_agg[(col, 'count')]
                if col == 'Mean_General':
                    final_df['Sample_Size'] = full_agg[(col, 'count')]

            # Finalize
            final_df = final_df.reset_index()
            final_df['Region'] = final_df['UF'].map(UF_REGION_MAP)
            final_df['Year'] = int(self.year)
            final_df['Grade'] = filter_mode
            
            if 'Mean_General' in final_df.columns:
                final_df = final_df.sort_values('Mean_General', ascending=False)

            base_name = f"enem_table_{self.year}_{filter_mode}"
            csv_path = os.path.join(DATA_PROCESSED, f"{base_name}.csv")
            xlsx_path = os.path.join(REPORT_XLSX, f"{base_name}.xlsx")

            final_df.to_csv(csv_path, index=False)
            final_df.to_excel(xlsx_path, index=False)
            
            print(f"   [SUCCESS] Processed {kept_rows} students.")
            print(f"     -> {base_name}.xlsx")

        except Exception as e:
            print(f"   [CRITICAL ERROR] {e}")
            self.logger.error(str(e))
            import traceback
            traceback.print_exc()

def main():
    os.system('cls' if os.name == 'nt' else 'clear')
    print("=== ENEM UNIFIED PIPELINE v3.6 ===")
    raw = input_timeout(">> Confirm Target Years [Default: 2015, 2018, 2022]", timeout=10, default="2015, 2018, 2022")    
    try:
        years = [int(y.strip()) for y in raw.split(',')]
    except:
        years = [2015, 2018, 2022]

    for y in years:
        # Looking specifically inside data/raw/enem/
        path = os.path.join(DATA_RAW, f"microdados_enem_{y}.zip")
        if os.path.exists(path):
            EnemPipeline(y, path).process()
        else:
            print(f"[SKIP] Missing: {path}")

if __name__ == "__main__":
    main()