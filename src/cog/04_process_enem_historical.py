"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/cog/enem_unified_pipeline.py
ROLE:        Senior Data Science Advisor
DATE:        2026-01-10 (v2.0 - 3EM Focus)

CHANGELOG:
    - v2.0: Adds strict filtering for 'Concluintes' (High School Seniors).
            This creates the "ENEM 3EM" dataset comparable to SAEB 3EM.

DESCRIPTION:
    Unified pipeline to process ENEM Microdata (2015-2024+).
    Filters for TP_ST_CONCLUSAO == 2 (Graduating this year) to proxy 3rd Year HS.
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

# --- MAPPINGS ---
SCORE_MAP = {
    'NU_NOTA_CN': 'Natural_Sciences',
    'NU_NOTA_CH': 'Humanities',
    'NU_NOTA_LC': 'Language',
    'NU_NOTA_MT': 'Math',
    'NU_NOTA_REDACAO': 'Essay'
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

    def process(self):
        print(f"\n[INFO] Processing ENEM {self.year} (Target: 3EM Students)...")
        self.logger.info(f"START ENEM {self.year} | File: {self.file_path}")
        
        try:
            with zipfile.ZipFile(self.file_path, 'r') as z:
                target_filename = self.get_largest_csv(z)
                if not target_filename:
                    print(f"   [ERROR] No CSV found."); return

                # TP_ST_CONCLUSAO: 1=Done, 2=Graduating This Year (3EM), 3=Future(Treineiro), 4=Quit
                cols_to_load = ['SG_UF_PROVA', 'TP_ESCOLA', 'TP_ST_CONCLUSAO'] + list(SCORE_MAP.keys())
                chunk_size = 250000 
                agg_storage = [] 
                
                with z.open(target_filename) as f:
                    first_line = f.readline().decode('latin-1')
                    sep = ';' if first_line.count(';') > first_line.count(',') else ','
                    f.seek(0)

                    reader = pd.read_csv(f, sep=sep, encoding='latin-1', usecols=cols_to_load, chunksize=chunk_size)
                    
                    batch_idx = 0
                    total_rows = 0
                    filtered_rows = 0

                    for chunk in reader:
                        batch_idx += 1
                        total_rows += len(chunk)
                        
                        # --- FILTER: ONLY 3EM (CONCLUINTES) ---
                        # TP_ST_CONCLUSAO == 2
                        chunk = chunk[chunk['TP_ST_CONCLUSAO'] == 2].copy()
                        filtered_rows += len(chunk)
                        
                        if chunk.empty: continue

                        if batch_idx % 10 == 0:
                            print(f"   ... Processed {total_rows/1e6:.1f}M rows (Kept {filtered_rows/1e6:.1f}M 3EM)", end='\r')

                        # 1. Standardize
                        chunk = chunk.rename(columns={'SG_UF_PROVA': 'UF'})
                        chunk = chunk.rename(columns=SCORE_MAP)
                        score_cols = list(SCORE_MAP.values())

                        # 2. Clean Zeros
                        chunk[score_cols] = chunk[score_cols].replace(0, np.nan)
                        
                        # 3. Row Metrics
                        chunk['Mean_General'] = chunk[score_cols].mean(axis=1)
                        target_cols = score_cols + ['Mean_General']

                        # 4. Public/Private Map
                        conditions = [chunk['TP_ESCOLA'].isin([2]), chunk['TP_ESCOLA'].isin([3])]
                        choices = [1, 0] # 1=Public, 0=Private
                        chunk['Is_Public'] = np.select(conditions, choices, default=np.nan)

                        # 5. Aggregation
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
                print("\n   [WARN] No 3EM students found (check filter TP_ST_CONCLUSAO).")
                return

            print(f"\n   [INFO] Consolidating metrics for 3EM students...")
            full_agg = pd.concat(agg_storage).groupby(level=0).sum()
            final_df = pd.DataFrame(index=full_agg.index)
            target_cols = list(SCORE_MAP.values()) + ['Mean_General']
            
            for col in target_cols:
                sum_val = full_agg[(col, 'sum')]
                count_val = full_agg[(col, 'count')]
                sum_sq_val = full_agg[f"{col}_sq"]
                
                final_df[col] = sum_val / count_val
                # StdDev
                variance = (sum_sq_val / count_val) - (final_df[col] ** 2)
                final_df[f"{col}_std"] = np.sqrt(variance.clip(lower=0)) # Clip handles float precision errs

            # Network Stats
            final_df['Public_Share'] = full_agg['Public_Sum'] / full_agg['Network_Valid_Count']
            total_students = full_agg[('Essay', 'count')]
            final_df['Network_Data_Coverage'] = full_agg['Network_Valid_Count'] / total_students
            
            # --- SAVING (With 3EM Tag) ---
            final_df = final_df.reset_index()
            final_df['Region'] = final_df['UF'].map(UF_REGION_MAP)
            final_df['Year'] = str(self.year)
            final_df['Grade'] = '3EM' # Explicit Tag

            main_cols = ['Year', 'Region', 'UF', 'Grade', 'Mean_General', 'Mean_General_std', 'Public_Share', 'Network_Data_Coverage']
            score_cols = [c for c in final_df.columns if c in list(SCORE_MAP.values())]
            final_df = final_df[main_cols + score_cols].sort_values('Mean_General', ascending=False)

            fname = f"enem_table_{self.year}_3EM"
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
    print("=== ENEM UNIFIED PIPELINE v2.0 (3EM Only) ===")
    
    # 1. Years
    raw = input_timeout(">> Years (e.g. 2018, 2023)", timeout=5, default="2015, 2018, 2022, 2023")
    try:
        years = [int(y.strip()) for y in raw.split(',')]
    except:
        years = [2015, 2018, 2022, 2023]

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