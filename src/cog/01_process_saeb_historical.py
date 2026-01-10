"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/cog/saeb_unified_pipeline.py
ROLE:        Senior Data Science Advisor
DATE:        2026-01-10 (v5.0 - Smart Batch Processing)

DESCRIPTION:
    Unified pipeline to process SAEB Microdata.
    Supports Batch Processing: Inputs multiple years, checks for default files,
    and interactively requests paths only for missing files.
"""

import pandas as pd
import numpy as np
import os
import zipfile
import sys
import logging

# --- GLOBAL CONFIG ---
BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_RAW = os.path.join(BASE_PATH, 'data', 'raw')
DATA_PROCESSED = os.path.join(BASE_PATH, 'data', 'processed')
REPORT_XLSX = os.path.join(BASE_PATH, 'reports', 'varcog', 'xlsx')
LOG_DIR = os.path.join(BASE_PATH, 'logs')

for p in [DATA_RAW, DATA_PROCESSED, REPORT_XLSX, LOG_DIR]:
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
        
        # Hints for known years (Fallback is Heuristic)
        self.column_hints = {
            2023: {'3EM': {'LP': 'MEDIA_EM_LP', 'MT': 'MEDIA_EM_MT'}},
            2021: {'3EM': {'LP': 'MEDIA_3EM_LP', 'MT': 'MEDIA_3EM_MT'}},
            2017: {
                '5EF': {'LP': 'MEDIA_5EF_LP', 'MT': 'MEDIA_5EF_MT'},
                '9EF': {'LP': 'MEDIA_9EF_LP', 'MT': 'MEDIA_9EF_MT'},
                '3EM': {'LP': 'MEDIA_3EM_LP', 'MT': 'MEDIA_3EM_MT'}
            }
        }

    def detect_separator(self, f_handle):
        try:
            line = f_handle.readline().decode('latin1')
            f_handle.seek(0)
            if line.count(';') > line.count(','): return ';'
            return ','
        except:
            f_handle.seek(0)
            return ';'

    def find_columns_heuristic(self, all_cols, grade_tag):
        upper_cols = [c.upper() for c in all_cols]
        lp_candidates = []
        mt_candidates = []

        for original, upper in zip(all_cols, upper_cols):
            if 'MEDIA' in upper or 'PROFICIENCIA' in upper:
                if grade_tag in upper or (grade_tag == '3EM' and '_EM_' in upper):
                    if 'LP' in upper or 'LINGUA' in upper:
                        lp_candidates.append(original)
                    elif 'MT' in upper or 'MAT' in upper:
                        mt_candidates.append(original)
        
        lp = min(lp_candidates, key=len) if lp_candidates else None
        mt = min(mt_candidates, key=len) if mt_candidates else None
        return lp, mt

    def process(self):
        # Configure Logger
        log_file = os.path.join(LOG_DIR, f'saeb_{self.year}.log')
        logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s | %(message)s', force=True)
        logging.info(f"START: Year {self.year} | Filter {self.filter_network} | File {os.path.basename(self.file_path)}")

        print(f"\n[INFO] Processing SAEB {self.year}...")
        
        try:
            with zipfile.ZipFile(self.file_path, 'r') as z:
                target = next((f for f in z.namelist() if 'TS_ESCOLA' in f and f.endswith('.csv')), None)
                if not target:
                    msg = "[ERROR] TS_ESCOLA csv not found in zip."
                    print(msg); logging.error(msg)
                    return

                with z.open(target) as f:
                    sep = self.detect_separator(f)
                    header = pd.read_csv(f, sep=sep, encoding='latin1', nrows=0).columns.tolist()
                    
                    col_adm = next((c for c in header if 'DEPENDENCIA' in c), 'ID_DEPENDENCIA_ADM')
                    col_uf = next((c for c in header if c in ['ID_UF', 'UF', 'CO_UF']), None)

                    grades_to_process = []
                    possible_grades = ['5EF', '9EF', '3EM']
                    
                    for g in possible_grades:
                        lp, mt = None, None
                        if self.year in self.column_hints and g in self.column_hints[self.year]:
                            lp = self.column_hints[self.year][g].get('LP')
                            mt = self.column_hints[self.year][g].get('MT')
                        
                        if not (lp in header and mt in header):
                            lp, mt = self.find_columns_heuristic(header, g)

                        if lp and mt:
                            grades_to_process.append({'grade': g, 'lp': lp, 'mt': mt})
                            logging.info(f"Grade {g} mapped: {lp}, {mt}")

                    if not grades_to_process:
                        print("[SKIP] No grades found.")
                        return

                    use_cols = [col_uf, col_adm] + [x['lp'] for x in grades_to_process] + [x['mt'] for x in grades_to_process]
                    use_cols = list(set([c for c in use_cols if c]))
                    
                    f.seek(0)
                    df = pd.read_csv(f, sep=sep, encoding='latin1', usecols=use_cols)

            # Filtering
            if self.filter_network == 'PUBLIC':
                df = df[df[col_adm].isin([1, 2, 3])]
            elif self.filter_network == 'PRIVATE':
                df = df[df[col_adm] == 4]

            # Standardization
            if pd.api.types.is_numeric_dtype(df[col_uf]):
                df['UF'] = df[col_uf].map(IBGE_TO_SIGLA)
            else:
                df['UF'] = df[col_uf]

            dfs_to_save = []
            for item in grades_to_process:
                c_lp, c_mt = item['lp'], item['mt']
                
                for c in [c_lp, c_mt]:
                    if df[c].dtype == object:
                        df[c] = df[c].astype(str).str.replace(',', '.', regex=False)
                    df[c] = pd.to_numeric(df[c], errors='coerce')

                sub = df.dropna(subset=[c_lp, c_mt]).copy()
                if sub.empty: continue

                grouped = sub.groupby('UF')[[c_lp, c_mt]].mean().reset_index()
                grouped.columns = ['UF', 'Language_Mean', 'Math_Mean']
                grouped['SAEB_General'] = (grouped['Language_Mean'] + grouped['Math_Mean']) / 2
                grouped['Grade'] = item['grade']
                grouped['Year'] = self.year
                grouped['Network'] = self.filter_network
                dfs_to_save.append(grouped)

            if dfs_to_save:
                final_df = pd.concat(dfs_to_save, ignore_index=True)
                cols = ['Year', 'Network', 'UF', 'Grade', 'SAEB_General', 'Math_Mean', 'Language_Mean']
                final_df = final_df[cols].sort_values(['Grade', 'SAEB_General'], ascending=[True, False])

                fname = f"saeb_unified_{self.year}_{self.filter_network.lower()}"
                csv_out = os.path.join(DATA_PROCESSED, f"{fname}.csv")
                xlsx_out = os.path.join(REPORT_XLSX, f"{fname}.xlsx")
                
                final_df.to_csv(csv_out, index=False)
                final_df.to_excel(xlsx_out, index=False)
                print(f"   -> Saved: {fname}")
                logging.info(f"Success. Saved {fname}")
            
        except Exception as e:
            print(f"[CRITICAL ERROR] {e}")
            logging.error(f"Critical: {e}")

def main():
    os.system('cls' if os.name == 'nt' else 'clear')
    print("="*60)
    print("   SAEB BATCH PROCESSOR (v5.0)")
    print("="*60)

    # 1. Get Years
    while True:
        raw = input(">> Enter years separated by comma (e.g. 2019, 2021, 2023): ")
        try:
            years = [int(y.strip()) for y in raw.split(',')]
            break
        except:
            print("[!] Invalid format. Use numeric years separated by commas.")

    # 2. Get Filter
    print("\n>> Select Network Filter for ALL years:")
    print("   [1] All Schools")
    print("   [2] Public Only (Recommended for Policy Analysis)")
    print("   [3] Private Only")
    opt = input("   Choice: ").strip()
    
    filter_map = {'2': 'PUBLIC', '3': 'PRIVATE'}
    selected_filter = filter_map.get(opt, 'ALL')

    # 3. Execution Loop
    print("\n" + "-"*60)
    for y in years:
        default_name = f"microdados_saeb_{y}.zip"
        default_path = os.path.join(DATA_RAW, default_name)
        
        final_path = None

        if os.path.exists(default_path):
            print(f"[CHECK] Year {y}: Found default file ({default_name})")
            final_path = default_path
        else:
            print(f"[CHECK] Year {y}: Default file NOT found.")
            while True:
                user_path = input(f"   >> Please paste full path for SAEB {y} zip: ").strip().replace('"', '')
                if os.path.exists(user_path) and zipfile.is_zipfile(user_path):
                    final_path = user_path
                    break
                print("   [!] Invalid path or not a zip file. Try again.")
        
        # Run Pipeline
        pipeline = SaebPipeline(y, final_path, selected_filter)
        pipeline.process()
    
    print("\n[DONE] Batch processing finished.")

if __name__ == "__main__":
    main()