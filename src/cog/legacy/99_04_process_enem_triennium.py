"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/cog/enem_unified_pipeline.py
SOURCE:      INEP (https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enem)
ROLE:        Senior Data Science Advisor
DATE:        2026-01-10 (v3.1 - Adaptive 2024 Fix)

CHANGELOG:
    - v3.1: Added fallback for missing 'TP_ST_CONCLUSAO' (skips 3EM filter if needed).
            Added support for 'TP_DEPENDENCIA_ADM_ESC' to calculate Public/Private.
    - v3.0: Robust header reading.

DESCRIPTION:
    Unified pipeline to process ENEM Microdata.
    Attempts to filter for 3EM, but gracefully degrades to 'All Data' if status column is missing.
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
        print(f"{prompt} [Auto in {timeout}s]: ", end='', flush=True)
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
                print(f"\n[TIMEOUT] Default assumed.")
                return default
            time.sleep(0.05)
except ImportError:
    def input_timeout(prompt, timeout=5, default=''):
        return input(f"{prompt} [Enter for Default]: ")

# --- GLOBAL CONFIG ---
BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_RAW = os.path.join(BASE_PATH, 'data', 'raw')
DATA_PROCESSED = os.path.join(BASE_PATH, 'data', 'processed')
REPORT_XLSX = os.path.join(BASE_PATH, 'reports', 'varcog', 'xlsx')
LOG_DIR = os.path.join(BASE_PATH, 'logs')

for p in [DATA_RAW, DATA_PROCESSED, REPORT_XLSX, LOG_DIR]:
    os.makedirs(p, exist_ok=True)

# --- STANDARD TARGET NAMES ---
TARGET_COLS = {
    'UF': ['SG_UF_PROVA', 'UF_PROVA', 'SG_UF_ESC'], # Added SG_UF_ESC as fallback
    'SCHOOL_TYPE': ['TP_ESCOLA'], 
    'SCHOOL_DEP': ['TP_DEPENDENCIA_ADM_ESC'], # New fallback for 2024
    'STATUS': ['TP_ST_CONCLUSAO'],
    'Natural_Sciences': ['NU_NOTA_CN'],
    'Humanities': ['NU_NOTA_CH'],
    'Language': ['NU_NOTA_LC'],
    'Math': ['NU_NOTA_MT'],
    'Essay': ['NU_NOTA_REDACAO']
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
        logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s', force=True)
        return logging.getLogger()

    def get_largest_csv(self, z):
        csv_files = [f for f in z.namelist() if f.lower().endswith('.csv')]
        if not csv_files: return None
        return sorted(csv_files, key=lambda x: z.getinfo(x).file_size, reverse=True)[0]

    def find_col_flexible(self, header, candidates):
        """Helper to find column names case-insensitively."""
        header_upper = {h.upper(): h for h in header}
        for cand in candidates:
            if cand.upper() in header_upper:
                return header_upper[cand.upper()]
        return None

    def process(self):
        print(f"\n[INFO] Processing ENEM {self.year}...")
        self.logger.info(f"START ENEM {self.year} | File: {self.file_path}")
        
        try:
            with zipfile.ZipFile(self.file_path, 'r') as z:
                target_filename = self.get_largest_csv(z)
                if not target_filename:
                    print(f"   [ERROR] No CSV found."); return

                with z.open(target_filename) as f:
                    # 1. Detect Separator & Read Header
                    first_line = f.readline().decode('latin-1')
                    sep = ';' if first_line.count(';') > first_line.count(',') else ','
                    f.seek(0)
                    
                    header = pd.read_csv(f, sep=sep, encoding='latin-1', nrows=0).columns.tolist()
                    if len(header) < 2:
                        sep = ',' if sep == ';' else ';'
                        f.seek(0)
                        header = pd.read_csv(f, sep=sep, encoding='latin-1', nrows=0).columns.tolist()
                    
                    print(f"   [DEBUG] Headers found (Top 5): {header[:5]}")

                    # 2. Map Actual Columns to Target Columns
                    col_map = {} 
                    missing_critical = []
                    
                    for internal_name, candidates in TARGET_COLS.items():
                        found = self.find_col_flexible(header, candidates)
                        if found:
                            col_map[found] = internal_name
                        else:
                            # Strict check only for UF. Others are adaptable.
                            if internal_name in ['UF']:
                                missing_critical.append(internal_name)
                    
                    if missing_critical:
                        print(f"   [CRITICAL ERROR] Missing mandatory columns: {missing_critical}")
                        return

                    # 3. Process Data
                    cols_to_load = list(col_map.keys())
                    chunk_size = 250000 
                    agg_storage = [] 
                    
                    f.seek(0)
                    reader = pd.read_csv(f, sep=sep, encoding='latin-1', usecols=cols_to_load, chunksize=chunk_size)
                    
                    batch_idx = 0
                    total_rows = 0
                    filtered_rows = 0
                    has_status_col = 'STATUS' in col_map.values()
                    
                    if not has_status_col:
                        print("   [WARN] 'TP_ST_CONCLUSAO' not found. 3EM Filter DISABLED (Processing ALL).")

                    for chunk in reader:
                        batch_idx += 1
                        total_rows += len(chunk)
                        
                        # Rename to Standard Internal Names
                        chunk = chunk.rename(columns=col_map)
                        
                        # --- FILTER: ONLY 3EM (If column exists) ---
                        if has_status_col:
                            chunk = chunk[chunk['STATUS'] == 2].copy()
                        
                        filtered_rows += len(chunk)
                        if chunk.empty: continue

                        if batch_idx % 10 == 0:
                            print(f"   ... Processed {total_rows/1e6:.1f}M rows (Active: {filtered_rows/1e6:.1f}M)", end='\r')

                        # 4. Clean Scores (0 -> NaN)
                        score_cols = ['Natural_Sciences', 'Humanities', 'Language', 'Math', 'Essay']
                        present_scores = [c for c in score_cols if c in chunk.columns]
                        
                        if present_scores:
                            chunk[present_scores] = chunk[present_scores].replace(0, np.nan)
                            chunk['Mean_General'] = chunk[present_scores].mean(axis=1)
                        else:
                            chunk['Mean_General'] = np.nan

                        target_cols = present_scores + ['Mean_General']

                        # 5. Public/Private Map (Hybrid Logic)
                        # Option A: Standard SCHOOL_TYPE (TP_ESCOLA) -> 2=Pub, 3=Priv
                        if 'SCHOOL_TYPE' in chunk.columns:
                            conditions = [chunk['SCHOOL_TYPE'].isin([2]), chunk['SCHOOL_TYPE'].isin([3])]
                            choices = [1, 0] 
                            chunk['Is_Public'] = np.select(conditions, choices, default=np.nan)
                        
                        # Option B: Dependency SCHOOL_DEP (TP_DEPENDENCIA_ADM_ESC) -> 1,2,3=Pub, 4=Priv
                        elif 'SCHOOL_DEP' in chunk.columns:
                             # 1=Federal, 2=Estadual, 3=Municipal -> PUBLIC (1)
                             # 4=Privada -> PRIVATE (0)
                            conditions = [chunk['SCHOOL_DEP'].isin([1, 2, 3]), chunk['SCHOOL_DEP'] == 4]
                            choices = [1, 0]
                            chunk['Is_Public'] = np.select(conditions, choices, default=np.nan)
                            
                        else:
                            chunk['Is_Public'] = np.nan

                        # 6. Aggregation
                        sq_cols = chunk[target_cols].pow(2)
                        sq_cols.columns = [f"{c}_sq" for c in target_cols]
                        chunk_sq = pd.concat([chunk[['UF']], sq_cols], axis=1)
                        
                        g_scores = chunk.groupby('UF')[target_cols].agg(['sum', 'count'])
                        g_sq = chunk_sq.groupby('UF')[[c for c in chunk_sq.columns if '_sq' in c]].sum()
                        g_net = chunk.groupby('UF')['Is_Public'].agg(['sum', 'count'])
                        g_net.columns = ['Public_Sum', 'Network_Valid_Count']

                        chunk_res = pd.concat([g_scores, g_sq, g_net], axis=1)
                        agg_storage.append(chunk_res)

            # --- CONSOLIDATION ---
            if not agg_storage:
                print("\n   [WARN] No data found.")
                return

            print(f"\n   [INFO] Consolidating metrics...")
            full_agg = pd.concat(agg_storage).groupby(level=0).sum()
            final_df = pd.DataFrame(index=full_agg.index)
            
            # Recalculate
            present_scores = [c for c in ['Natural_Sciences', 'Humanities', 'Language', 'Math', 'Essay'] if (c, 'sum') in full_agg.columns]
            target_cols = present_scores + ['Mean_General']
            
            for col in target_cols:
                sum_val = full_agg[(col, 'sum')]
                count_val = full_agg[(col, 'count')]
                sum_sq_val = full_agg[f"{col}_sq"]
                
                final_df[col] = sum_val / count_val
                variance = (sum_sq_val / count_val) - (final_df[col] ** 2)
                final_df[f"{col}_std"] = np.sqrt(variance.clip(lower=0)) 

            # Network Stats
            final_df['Public_Share'] = full_agg['Public_Sum'] / full_agg['Network_Valid_Count']
            
            if 'Essay' in present_scores:
                total_students = full_agg[('Essay', 'count')]
                final_df['Network_Data_Coverage'] = full_agg['Network_Valid_Count'] / total_students
            else:
                final_df['Network_Data_Coverage'] = np.nan
            
            # --- SAVING ---
            final_df = final_df.reset_index()
            final_df['Region'] = final_df['UF'].map(UF_REGION_MAP)
            final_df['Year'] = str(self.year)
            
            # Tag adjustment: if filtering was possible, mark as 3EM, else ALL
            final_df['Grade'] = '3EM' if has_status_col else 'ALL'

            cols = ['Year', 'Region', 'UF', 'Grade', 'Mean_General', 'Mean_General_std', 'Public_Share', 'Network_Data_Coverage'] + present_scores
            final_df = final_df[[c for c in cols if c in final_df.columns]].sort_values('Mean_General', ascending=False)

            fname = f"enem_table_{self.year}_{final_df['Grade'].iloc[0]}"
            csv_path = os.path.join(DATA_PROCESSED, f"{fname}.csv")
            xlsx_path = os.path.join(REPORT_XLSX, f"{fname}.xlsx")
            
            final_df.to_csv(csv_path, index=False)
            final_df.to_excel(xlsx_path, index=False)
            
            print(f"   -> Saved: {fname}.xlsx")
            self.logger.info(f"SUCCESS. Saved {fname}")

        except Exception as e:
            print(f"[CRITICAL ERROR] {e}")
            self.logger.error(str(e))
            import traceback
            traceback.print_exc()

def main():
    os.system('cls' if os.name == 'nt' else 'clear')
    print("=== ENEM UNIFIED PIPELINE v3.1 (Adaptive 2024) ===")
    
    raw = input_timeout(">> Years (e.g. 2024)", timeout=5, default="2015, 2018, 2022, 2023, 2024")
    try:
        years = [int(y.strip()) for y in raw.split(',')]
    except:
        years = [2015, 2018, 2022, 2023, 2024]

    print(f"\n[QUEUE] Processing: {years}")
    print("-" * 50)

    for y in years:
        default_path = os.path.join(DATA_RAW, f"microdados_enem_{y}.zip")
        final_path = None
        
        if os.path.exists(default_path):
            final_path = default_path
        else:
            user_path = input(f"   >> Missing {y} (Path or Enter to Skip): ").strip().replace('"', '')
            if user_path and os.path.exists(user_path):
                final_path = user_path
        
        if final_path:
            pipeline = EnemPipeline(y, final_path)
            pipeline.process()

    print("\n[DONE] Pipeline finished.")

if __name__ == "__main__":
    main()