"""
================================================================================
PROJECT:        COGNITIVE CAPITAL ANALYSIS - BRAZIL
SCRIPT:         src/cog/cog_02_process_enem_unified_pipeline.py
VERSION:        3.7 (Production - Student Count / N_Students Extraction)
DATE:           2026-01-26
--------------------------------------------------------------------------------
PRINCIPAL INVESTIGATOR:  Dr. José Aparecido da Silva
LEAD DATA SCIENTIST:     Me. Cássio Dalbem Barth
SOURCE:                  INEP Microdata (Instituto Nacional de Estudos e Pesquisas)
================================================================================

ABSTRACT:
    Unified ETL pipeline for processing INEP ENEM student-level microdata. 
    Refactored to extract and standardize Student Count (N_Students) for 
    Cognitive Capital validation.

    v3.7 CHANGE LOG:
    - Feature: Added explicit 'N_Students' extraction.
    - Consistency: Aligned output columns with SAEB/PISA ecosystem.
"""

import pandas as pd
import numpy as np
import os
import zipfile
import logging
import time
import warnings

warnings.filterwarnings("ignore")

# --- GLOBAL CONFIG & DIRECTORY STRUCTURE ---
BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_RAW = os.path.join(BASE_PATH, 'data', 'raw', 'enem') 
DATA_PROCESSED = os.path.join(BASE_PATH, 'data', 'processed')
REPORT_XLSX = os.path.join(BASE_PATH, 'reports', 'varcog', 'xlsx')
LOG_DIR = os.path.join(BASE_PATH, 'logs')

for p in [DATA_RAW, DATA_PROCESSED, REPORT_XLSX, LOG_DIR]:
    os.makedirs(p, exist_ok=True)

TARGET_COLS = {
    'UF': ['SG_UF_PROVA', 'UF_PROVA', 'SG_UF_ESC'], 
    'SCHOOL_ID': ['CO_ESCOLA'], 
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
        logging.basicConfig(filename=log_file, level=logging.INFO, 
                            format='%(asctime)s | %(levelname)s | %(message)s', force=True)
        return logging.getLogger()

    def get_largest_csv(self, z):
        csv_files = [f for f in z.namelist() if f.lower().endswith('.csv')]
        return sorted(csv_files, key=lambda x: z.getinfo(x).file_size, reverse=True)[0] if csv_files else None

    def find_col_flexible(self, header, candidates):
        header_upper = {h.upper(): h for h in header}
        for cand in candidates:
            if cand.upper() in header_upper: return header_upper[cand.upper()]
        return None

    def process(self):
        print(f"\n[START] Processing ENEM {self.year}...")
        
        try:
            with zipfile.ZipFile(self.file_path, 'r') as z:
                target_filename = self.get_largest_csv(z)
                with z.open(target_filename) as f:
                    first_line = f.readline().decode('latin-1')
                    sep = ';' if first_line.count(';') > first_line.count(',') else ','
                    f.seek(0)
                    header = pd.read_csv(f, sep=sep, encoding='latin-1', nrows=0).columns.tolist()
                    
                    col_map = {}
                    for k, v in TARGET_COLS.items():
                        found = self.find_col_flexible(header, v)
                        if found: col_map[found] = k

                    filter_mode = 'STRICT_3EM' if 'STATUS' in col_map.values() else 'PROXY_3EM'

            # PASS 2: AGGREGATION
            with zipfile.ZipFile(self.file_path, 'r') as z:
                with z.open(target_filename) as f:
                    reader = pd.read_csv(f, sep=sep, encoding='latin-1', usecols=list(col_map.keys()), chunksize=300000)
                    agg_storage = []
                    score_cols = ['Natural_Sciences', 'Humanities', 'Language', 'Math', 'Essay']

                    for chunk in reader:
                        chunk = chunk.rename(columns=col_map)
                        if filter_mode == 'STRICT_3EM':
                            chunk = chunk[chunk['STATUS'] == 2].copy()
                        else:
                            chunk = chunk[chunk['SCHOOL_ID'].notna()].copy()
                        
                        if chunk.empty: continue

                        # Clean Scores
                        valid_present = [c for c in score_cols if c in chunk.columns]
                        chunk[valid_present] = chunk[valid_present].apply(pd.to_numeric, errors='coerce')
                        chunk['Mean_General'] = chunk[valid_present].mean(axis=1)

                        # Logic: Aggregating sums and counts to calculate weighted mean later
                        # Added N_Students as a direct count of the UF column
                        chunk['N_Students'] = 1 
                        
                        group_cols = valid_present + ['Mean_General', 'N_Students']
                        agg_chunk = chunk.groupby('UF')[group_cols].agg(['sum'])
                        agg_storage.append(agg_chunk)

            # CONSOLIDATION
            full_agg = pd.concat(agg_storage).groupby(level=0).sum()
            final_df = pd.DataFrame(index=full_agg.index)
            
            # Calculate final means from sums and total N
            # Note: N_Students is now the sum of our counter
            total_n = full_agg[('N_Students', 'sum')]
            
            for col in full_agg.columns.levels[0]:
                if col == 'N_Students':
                    final_df['N_Students'] = full_agg[(col, 'sum')]
                else:
                    # We use the count of non-nulls for specific scores if available, 
                    # but here we use the total filtered N for the general mean
                    final_df[col] = full_agg[(col, 'sum')] / total_n

            final_df = final_df.reset_index()
            final_df['Region'] = final_df['UF'].map(UF_REGION_MAP)
            final_df['Year'] = int(self.year)
            final_df['Grade'] = filter_mode
            
            final_df = final_df[['Year', 'Region', 'UF', 'Grade', 'Mean_General', 'N_Students'] + score_cols]
            final_df = final_df.sort_values('Mean_General', ascending=False)

            # Output paths
            base_name = f"enem_table_{self.year}_{filter_mode}"
            final_df.to_csv(os.path.join(DATA_PROCESSED, f"{base_name}.csv"), index=False)
            final_df.to_excel(os.path.join(REPORT_XLSX, f"{base_name}.xlsx"), index=False)
            
            print(f"   [SUCCESS] Processed {int(final_df['N_Students'].sum())} students.")

        except Exception as e:
            print(f"   [CRITICAL ERROR] {e}")

if __name__ == "__main__":
    # Execução para os anos padrão
    for year in [2015, 2018, 2022]:
        path = os.path.join(DATA_RAW, f"microdados_enem_{year}.zip")
        if os.path.exists(path):
            EnemPipeline(year, path).process()