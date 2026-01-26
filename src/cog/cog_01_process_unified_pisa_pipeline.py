"""
================================================================================
PROJECT:        COGNITIVE CAPITAL ANALYSIS - BRAZIL
SCRIPT:         src/cog/saeb_unified_pipeline.py
VERSION:        14.3 (Production - Direct Extraction / Multi-Cycle / Subdir Support)
DATE:           2026-01-16
--------------------------------------------------------------------------------
PRINCIPAL INVESTIGATOR:  Dr. José Aparecido da Silva
LEAD DATA SCIENTIST:     Me. Cássio Dalbem Barth
SOURCE:                  INEP Microdata (SAEB / Prova Brasil)
================================================================================

ABSTRACT:
    Unified ETL pipeline for processing SAEB (System of Assessment of Basic 
    Education) school-level microdata. Handles architectural shifts across 
    cycles (2015-2023) while maintaining statistical integrity.
    
    Key Features:
    1. Focuses on critical transition points: 9th Grade (9EF) and 3rd Year High School (3EM).
    2. Calculates 'Mean_General_SAEB' as the arithmetic mean of Language and Math.
    3. Auto-detects and normalizes administrative dependency (Public vs Private).
    4. Outputs standardized CSV matrices for ecosystem validation.

DATA SOURCE CITATION:
    - Source: INEP - Instituto Nacional de Estudos e Pesquisas Educacionais Anísio Teixeira
    - Portal: https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/saeb
    - Access: 2026-01-16
    - Files:  microdados_saeb_[year].zip

RASTREABILITY SETTINGS:
    - INPUT_ROOT:  data/raw/saeb/
    - OUTPUT_CSV:  data/processed/saeb_table_[year]_[grade].csv
    - LOG_FILE:    logs/saeb_pipeline_[year].log
    - METHOD:      Arithmetic mean of pre-calculated school proficiencies (LP/MT).
                   Aggregation by Federal Unit (UF).

DEPENDENCIES:
    pandas, numpy, zipfile, logging, os
================================================================================
"""

import pandas as pd
import numpy as np
import os
import zipfile
import logging
import warnings

# Suppress warnings for cleaner console output
warnings.filterwarnings('ignore')

# --- GLOBAL CONFIGURATION ---
BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Input/Output Paths
DIR_RAW = os.path.join(BASE_PATH, 'data', 'raw', 'saeb')
DIR_PROCESSED = os.path.join(BASE_PATH, 'data', 'processed')
LOG_DIR = os.path.join(BASE_PATH, 'logs')

# Ensure directory structure exists
for p in [DIR_RAW, DIR_PROCESSED, LOG_DIR]:
    os.makedirs(p, exist_ok=True)

# IBGE State Mapping (Code -> Sigla)
IBGE_MAP = {
    11:'RO', 12:'AC', 13:'AM', 14:'RR', 15:'PA', 16:'AP', 17:'TO',
    21:'MA', 22:'PI', 23:'CE', 24:'RN', 25:'PB', 26:'PE', 27:'AL', 28:'SE', 29:'BA',
    31:'MG', 32:'ES', 33:'RJ', 35:'SP', 41:'PR', 42:'SC', 43:'RS',
    50:'MS', 51:'MT', 52:'GO', 53:'DF'
}

# Column Heuristics (Mapping naming variations across years)
# Structure: Year -> Grade -> {Subject: Column_Name_Fragment}
COLUMN_MAPPING = {
    2023: {
        '9EF': {'LP': 'MEDIA_9EF_LP', 'MT': 'MEDIA_9EF_MT'},
        '3EM': {'LP': 'MEDIA_EM_LP',  'MT': 'MEDIA_EM_MT'} # Note: 'EM' instead of '3EM'
    },
    2021: {
        '9EF': {'LP': 'MEDIA_9EF_LP', 'MT': 'MEDIA_9EF_MT'},
        '3EM': {'LP': 'MEDIA_3EM_LP', 'MT': 'MEDIA_3EM_MT'}
    },
    2019: {
        '9EF': {'LP': 'MEDIA_9EF_LP', 'MT': 'MEDIA_9EF_MT'},
        '3EM': {'LP': 'MEDIA_3EM_LP', 'MT': 'MEDIA_3EM_MT'}
    },
    'LEGACY': { 
        '9EF': {'LP': 'MEDIA_9EF_LP', 'MT': 'MEDIA_9EF_MT'},
        '3EM': {'LP': 'MEDIA_3EM_LP', 'MT': 'MEDIA_3EM_MT'}
    }
}

class SaebPipeline:
    def __init__(self, year, filename):
        self.year = year
        self.filename = filename
        self.filepath = os.path.join(DIR_RAW, filename)
        
        # Configure unique logger per run
        logging.basicConfig(
            filename=os.path.join(LOG_DIR, f'saeb_pipeline_{self.year}.log'),
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            force=True
        )

    def detect_separator(self, f_handle):
        """Auto-detects CSV separator (; or ,) by reading the first line."""
        try:
            line = f_handle.readline().decode('latin1')
            f_handle.seek(0)
            if line.count(';') > line.count(','): return ';'
            return ','
        except:
            f_handle.seek(0)
            return ';'

    def normalize_network(self, val):
        """
        Standardizes 'Dependencia Administrativa':
        1 (Federal), 2 (State), 3 (Municipal) -> 1 (Public)
        4 (Private) -> 0 (Private)
        """
        try:
            val = int(val)
            if val in [1, 2, 3]: return 1 # Public
            if val == 4: return 0         # Private
        except: pass
        return np.nan

    def process(self):
        print(f"[*] Starting processing for SAEB {self.year}...")
        logging.info(f"Processing file: {self.filepath}")

        if not os.path.exists(self.filepath):
            msg = f"File not found: {self.filepath}. Please ensure it is in {DIR_RAW}"
            print(f" [!] {msg}")
            logging.error(msg)
            return

        try:
            with zipfile.ZipFile(self.filepath, 'r') as z:
                # Search for the main school table (TS_ESCOLA)
                target_file = next((f for f in z.namelist() if 'TS_ESCOLA' in f and f.endswith('.csv')), None)
                
                if not target_file:
                    logging.error("TS_ESCOLA.csv not found inside zip archive.")
                    return

                print(f" -> Found internal dataset: {target_file}")
                
                with z.open(target_file) as f:
                    sep = self.detect_separator(f)
                    header = pd.read_csv(f, sep=sep, encoding='latin1', nrows=0).columns.tolist()
                    
                    # 1. Identify Network/Administration Column
                    col_net = next((h for h in header if 'ID_DEPENDENCIA' in h.upper() or 'TP_DEPENDENCIA' in h.upper()), None)
                    
                    # 2. Identify UF/State Column
                    col_uf = next((h for h in header if h.upper() in ['ID_UF', 'CO_UF', 'UF', 'SG_UF']), None)

                    # 3. Identify Grade Columns based on mapping
                    mapping = COLUMN_MAPPING.get(self.year, COLUMN_MAPPING['LEGACY'])
                    
                    grades_found = []
                    for grade in ['9EF', '3EM']:
                        lp_hint = mapping[grade]['LP']
                        mt_hint = mapping[grade]['MT']
                        # Flexible search
                        col_lp = next((h for h in header if lp_hint in h.upper()), None)
                        col_mt = next((h for h in header if mt_hint in h.upper()), None)
                        
                        if col_lp and col_mt:
                            grades_found.append({'grade': grade, 'col_lp': col_lp, 'col_mt': col_mt})
                    
                    if not grades_found:
                        logging.warning(f"No grade columns found matching criteria for {self.year}")
                        return

                    # Load Data (Optimized: only necessary columns)
                    use_cols = [col_uf, col_net] + [g['col_lp'] for g in grades_found] + [g['col_mt'] for g in grades_found]
                    use_cols = [c for c in use_cols if c]
                    
                    f.seek(0)
                    df = pd.read_csv(f, sep=sep, encoding='latin1', usecols=use_cols)

            # --- DATA TRANSFORMATION ---
            
            # Normalize UF (Integer to String Sigla)
            if pd.api.types.is_numeric_dtype(df[col_uf]): df['UF'] = df[col_uf].map(IBGE_MAP)
            else: df['UF'] = df[col_uf]

            # Normalize Public/Private Status
            if col_net: df['Is_Public'] = df[col_net].apply(self.normalize_network)

            # Process each Grade Cycle
            for g_info in grades_found:
                grade = g_info['grade']
                c_lp = g_info['col_lp']
                c_mt = g_info['col_mt']

                # Convert scores to numeric (handling comma decimals)
                df[c_lp] = pd.to_numeric(df[c_lp].astype(str).str.replace(',', '.', regex=False), errors='coerce')
                df[c_mt] = pd.to_numeric(df[c_mt].astype(str).str.replace(',', '.', regex=False), errors='coerce')

                # Filter valid rows
                sub_df = df.dropna(subset=[c_lp, c_mt]).copy()
                if sub_df.empty: continue

                # AGGREGATION: Mean by State (UF)
                agg = sub_df.groupby('UF')[[c_lp, c_mt]].mean().reset_index()
                
                # Standardization of Output Columns
                agg.columns = ['UF', 'Language', 'Math']
                agg['Mean_General'] = (agg['Language'] + agg['Math']) / 2
                agg['Year'] = self.year
                agg['Grade'] = grade
                agg['Region'] = agg['UF'].map(lambda x: self.get_region(x))

                # Save Artifact
                filename_out = f"saeb_table_{self.year}_{grade}.csv"
                path_out = os.path.join(DIR_PROCESSED, filename_out)
                
                final_cols = ['Region', 'UF', 'Year', 'Grade', 'Language', 'Math', 'Mean_General']
                agg[final_cols].sort_values('Mean_General', ascending=False).to_csv(path_out, index=False)
                
                print(f" -> Generated Artifact: {filename_out} (Records: {len(agg)})")
                logging.info(f"Successfully generated {filename_out}")

        except Exception as e:
            logging.error(f"Critical Pipeline Error: {str(e)}")
            print(f" [ERROR] {e}")

    def get_region(self, uf):
        """Maps UF to Macro-Region."""
        regions = {
            'North': ['AM', 'RR', 'AP', 'PA', 'TO', 'RO', 'AC'],
            'Northeast': ['MA', 'PI', 'CE', 'RN', 'PB', 'PE', 'AL', 'SE', 'BA'],
            'Center-West': ['MT', 'MS', 'GO', 'DF'],
            'Southeast': ['SP', 'RJ', 'ES', 'MG'],
            'South': ['PR', 'RS', 'SC']
        }
        for reg, ufs in regions.items():
            if uf in ufs: return reg
        return 'Unknown'

if __name__ == "__main__":
    print("\n" + "="*80)
    print(" SAEB UNIFIED PIPELINE | COGNITIVE CAPITAL ANALYSIS")
    print("="*80)
    print(f"Reading from: {DIR_RAW}")
    print(f"Writing to:   {DIR_PROCESSED}")
    print("-" * 80)
    
    # Execution Plan
    tasks = [
        (2015, 'microdados_saeb_2015.zip'),
        (2017, 'microdados_saeb_2017.zip'),
        (2019, 'microdados_saeb_2019.zip'),
        (2021, 'microdados_saeb_2021.zip'),
        (2023, 'microdados_saeb_2023.zip')
    ]

    for year, fname in tasks:
        processor = SaebPipeline(year, fname)
        processor.process()

    print("\n" + "="*80)
    print(" PIPELINE EXECUTION COMPLETED")
    print("="*80)python