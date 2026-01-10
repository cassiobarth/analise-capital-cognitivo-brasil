"""
================================================================================
PROJECT:       COGNITIVE CAPITAL ANALYSIS - BRAZIL
SCRIPT:        src/cog/process_pisa_unified.py
VERSION:       7.1 (Fix: Force Raw Codes for 2018 Stratum)
DATE:          2026-01-10
--------------------------------------------------------------------------------
PRINCIPAL INVESTIGATOR:  Dr. José Aparecido da Silva
LEAD DEVELOPER:          Me. Cássio Dalbem Barth
================================================================================

ABSTRACT:
    Unified ETL pipeline handling the specific data architecture of each PISA cycle.
    
    CRITICAL FIX v7.1:
    - Added 'convert_categoricals=False' to PISA 2018 loader.
    - This prevents SPSS from masking the 'BRAxxxxx' stratum codes with text labels,
      ensuring the positional extraction (Region slicing) works correctly.

DEPENDENCIES:
    pandas, numpy, pyreadstat, openpyxl, re
================================================================================
"""

import pandas as pd
import numpy as np
import os
import sys
import time
import pyreadstat
from pathlib import Path
from datetime import timedelta

# --- 1. SETUP & PATHS ---

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent.parent
DATA_RAW_ROOT = PROJECT_ROOT / 'data' / 'raw' / 'Pisa'
REPORT_DIR = PROJECT_ROOT / 'reports' / 'varcog'
CSV_OUT_DIR = PROJECT_ROOT / 'data' / 'processed'
XLSX_OUT_DIR = REPORT_DIR / 'xlsx'

for path in [CSV_OUT_DIR, XLSX_OUT_DIR]:
    path.mkdir(parents=True, exist_ok=True)

# SafeGuard Import (Active only for 2015)
script_dir = os.path.dirname(os.path.abspath(__file__))
lib_path = os.path.join(script_dir, 'lib')
if lib_path not in sys.path: sys.path.append(lib_path)
try:
    from safeguard import DataGuard
except ImportError:
    DataGuard = None

# --- 2. HELPERS ---

class ExecutionTimer:
    """Scientific timer for measuring processing duration."""
    def __enter__(self):
        self.start = time.perf_counter()
        return self
    def __exit__(self, *args):
        self.interval = time.perf_counter() - self.start
        print(f"[TIMER] Execution time: {str(timedelta(seconds=self.interval))}")

# --- 3. CORE ETL CLASS ---

class PisaUnifiedETL:
    
    def run_2015(self):
        """
        CYCLE: 2015
        GRANULARITY: STATE (UF)
        METHOD: VALUE LABELS + TEXT MATCHING
        """
        print("="*60)
        print("[START] PISA 2015 Extraction (Granularity: STATE/UF)")
        print("="*60)
        
        base_path = DATA_RAW_ROOT / 'pisa_2015'
        if not base_path.exists(): print(f"[ERROR] Path missing: {base_path}"); return

        sav_files = [f for f in os.listdir(base_path) if 'STU' in f and f.endswith('.sav')]
        if not sav_files: print("[CRITICAL] No .sav file found."); return

        target_file = base_path / sav_files[0]
        print(f"[FILE] Reading: {sav_files[0]}")

        # 2015 Specific Dictionaries (Strict Copy from Validated Script)
        NAME_TO_IBGE = {
            'RONDONIA': 11, 'RONDÔNIA': 11, 'ACRE': 12, 'AMAZONAS': 13, 'RORAIMA': 14,
            'PARA': 15, 'PARÁ': 15, 'AMAPA': 16, 'AMAPÁ': 16, 'TOCANTINS': 17,
            'MARANHAO': 21, 'MARANHÃO': 21, 'PIAUI': 22, 'PIAUÍ': 22, 'CEARA': 23, 'CEARÁ': 23,
            'RIO GRANDE DO NORTE': 24, 'PARAIBA': 25, 'PARAÍBA': 25, 'PERNAMBUCO': 26,
            'ALAGOAS': 27, 'SERGIPE': 28, 'BAHIA': 29,
            'MINAS GERAIS': 31, 'ESPIRITO SANTO': 32, 'ESPÍRITO SANTO': 32,
            'RIO DE JANEIRO': 33, 'SAO PAULO': 35, 'SÃO PAULO': 35,
            'PARANA': 41, 'PARANÁ': 41, 'SANTA CATARINA': 42, 'RIO GRANDE DO SUL': 43,
            'MATO GROSSO DO SUL': 50, 'MATO GROSSO': 51, 'GOIAS': 52, 'GOIÁS': 52, 'DISTRITO FEDERAL': 53
        }
        REGIONAL_MAP = {'N': [11,12,13,14,15,16,17], 'NE': [21,22,23,24,25,26,27,28,29], 'SE': [31,32,33,35], 'S': [41,42,43], 'CO': [50,51,52,53]}
        IBGE_TO_REGION = {code: reg for reg, codes in REGIONAL_MAP.items() for code in codes}
        IBGE_TO_SIGLA = {11:'RO', 12:'AC', 13:'AM', 14:'RR', 15:'PA', 16:'AP', 17:'TO', 21:'MA', 22:'PI', 23:'CE', 24:'RN', 25:'PB', 26:'PE', 27:'AL', 28:'SE', 29:'BA', 31:'MG', 32:'ES', 33:'RJ', 35:'SP', 41:'PR', 42:'SC', 43:'RS', 50:'MS', 51:'MT', 52:'GO', 53:'DF'}

        def resolve_ibge_from_text(text_label):
            if not isinstance(text_label, str): return None
            text_upper = text_label.upper()
            sorted_names = sorted(NAME_TO_IBGE.keys(), key=len, reverse=True)
            for name in sorted_names:
                if name in text_upper: return NAME_TO_IBGE[name]
            return None

        try:
            _, meta = pyreadstat.read_sav(str(target_file), metadataonly=True)
            candidates = ['STRATUM', 'REGION', 'CNT', 'ST004D01T']
            region_col = next((c for c in candidates if c in meta.column_names), None)
            scores = [c for c in meta.column_names if c.startswith('PV1') and any(x in c for x in ['MATH', 'READ', 'SCIE'])]
            
            use_cols = list(set([region_col] + scores))
            if 'CNT' in meta.column_names: use_cols.append('CNT')

            print(f"[INFO] Loading {len(use_cols)} columns...")
            df, meta = pyreadstat.read_sav(str(target_file), usecols=use_cols)
            
            if 'CNT' in df.columns:
                df = df[df['CNT'] == 'BRA'].copy()

            if region_col in meta.variable_value_labels:
                labels = meta.variable_value_labels[region_col]
                df['STRATUM_TEXT'] = df[region_col].map(labels).fillna(df[region_col].astype(str))
            else:
                df['STRATUM_TEXT'] = df[region_col].astype(str)

            df['IBGE_CODE'] = df['STRATUM_TEXT'].apply(resolve_ibge_from_text)
            print(f"[STATS] Mapped Rows: {df['IBGE_CODE'].notnull().sum()}/{len(df)}")
            
            df = df.dropna(subset=['IBGE_CODE'])
            if 'PV1MATH' in df.columns: df['Math'] = df['PV1MATH']
            if 'PV1READ' in df.columns: df['Read'] = df['PV1READ']
            if 'PV1SCIE' in df.columns: df['Science'] = df['PV1SCIE']

            summary = df.groupby('IBGE_CODE')[['Math', 'Read', 'Science']].mean().reset_index()
            summary['UF'] = summary['IBGE_CODE'].map(IBGE_TO_SIGLA)
            summary['Region'] = summary['IBGE_CODE'].map(IBGE_TO_REGION)
            summary['Cognitive_Global_Mean'] = summary[['Math', 'Read', 'Science']].mean(axis=1)
            summary = summary.sort_values('Cognitive_Global_Mean', ascending=False)
            summary = summary[['Region', 'UF', 'Math', 'Read', 'Science', 'Cognitive_Global_Mean']]

            if DataGuard:
                print("[AUDIT] Verifying Consistency...")
                guard = DataGuard(summary, "PISA 2015")
                guard.check_historical_consistency('Cognitive_Global_Mean', 'UF')
                guard.validate(strict=True)

            out_csv = CSV_OUT_DIR / 'pisa_2015_states.csv'
            out_xlsx = XLSX_OUT_DIR / 'pisa_2015_states.xlsx'
            summary.to_csv(out_csv, index=False)
            summary.to_excel(out_xlsx, index=False)
            print(f"[SUCCESS] Saved: {out_csv.name}")

        except Exception as e:
            print(f"[CRITICAL FAILURE] {e}")

    def run_2018(self):
        """
        CYCLE: 2018
        GRANULARITY: MACRO-REGION
        METHOD: STRATUM NUMERIC PARSING (BRAxxYY)
        """
        print("="*60)
        print("[START] PISA 2018 Extraction (Granularity: REGION)")
        print("="*60)
        
        RAW_FILE = DATA_RAW_ROOT / 'pisa_2018' / 'CY07_MSU_STU_QQQ.sav'
        if not RAW_FILE.exists(): print(f"[ERROR] File not found: {RAW_FILE}"); return

        print(f"[INFO] Loading PISA 2018: {RAW_FILE.name}")
        cols = ['CNT', 'STRATUM', 'PV1MATH', 'PV1READ', 'PV1SCIE']
        
        try:
            # FIX: convert_categoricals=False forces pandas to read 'BRA0206' instead of the label
            df = pd.read_spss(str(RAW_FILE), usecols=cols, convert_categoricals=False)
            
            # Filter Brazil (Handles 'BRA' string or 76 numeric)
            df = df[df['CNT'].astype(str).str.contains('BRA|Brazil|76', case=False, na=False)].copy()
            print(f"      - Brazil rows found: {len(df)}")
            
            if len(df) == 0: print("[ERROR] No Brazil rows found."); return

            # --- 2018 LOGIC: STRATUM DIGITS ---
            def get_region_code_2018(stratum):
                s = str(stratum).upper().strip()
                if s.startswith('BRA'):
                    # PISA 2018: BRA + Region(2) + Stratum(2)
                    # Extract positions 3 and 4 (0-based) -> BRA(012) XX(34)
                    code = s[3:5] 
                    if code == '01': return 'North'
                    if code == '02': return 'Northeast'
                    if code == '03': return 'Southeast'
                    if code == '04': return 'South'
                    if code == '05': return 'Center-West'
                return 'UNKNOWN'

            print("[INFO] Decoding Regions from numeric Stratum...")
            df['Region'] = df['STRATUM'].apply(get_region_code_2018)
            
            # Validation
            unknowns = len(df[df['Region'] == 'UNKNOWN'])
            valid = len(df[df['Region'] != 'UNKNOWN'])
            print(f"[STATS] Valid Regional Rows: {valid}")
            
            if valid == 0:
                print("[CRITICAL] Region mapping failed. Checking Stratum sample:")
                print(df['STRATUM'].head().tolist())
                return

            df = df[df['Region'] != 'UNKNOWN']
            
            means = df.groupby('Region')[['PV1MATH', 'PV1READ', 'PV1SCIE']].mean().reset_index()
            counts = df['Region'].value_counts().reset_index()
            counts.columns = ['Region', 'Student_Count']
            
            res = pd.merge(counts, means, on='Region')
            res['Cognitive_Global_Mean'] = (res['PV1MATH'] + res['PV1READ'] + res['PV1SCIE']) / 3
            res = res.rename(columns={'PV1MATH': 'Math_Mean', 'PV1READ': 'Read_Mean', 'PV1SCIE': 'Science_Mean'})
            
            df_final = res.round(2).sort_values('Cognitive_Global_Mean', ascending=False)
            
            csv_path = CSV_OUT_DIR / 'pisa_2018_regional_summary.csv'
            xlsx_path = XLSX_OUT_DIR / 'pisa_2018_regional_summary.xlsx'
            df_final.to_csv(csv_path, index=False)
            df_final.to_excel(xlsx_path, index=False)
            print(f"[SUCCESS] Saved: {csv_path.name}")

        except Exception as e:
            print(f"[ERROR] {e}")

    def run_2022(self):
        """
        CYCLE: 2022
        GRANULARITY: MACRO-REGION
        METHOD: SEMANTIC TEXT MATCHING
        """
        print("="*60)
        print("[START] PISA 2022 Extraction (Granularity: REGION)")
        print("="*60)

        RAW_FILE = DATA_RAW_ROOT / 'pisa_2022' / 'CY08MSP_STU_QQQ.sav'
        if not RAW_FILE.exists(): print(f"[ERROR] File not found: {RAW_FILE}"); return

        print(f"[INFO] Loading PISA 2022: {RAW_FILE.name}")
        cols = ['CNT', 'STRATUM', 'PV1MATH', 'PV1READ', 'PV1SCIE']
        
        try:
            # 2022 is usually safer with default categoricals due to text matching
            df = pd.read_spss(str(RAW_FILE), usecols=cols)
            df = df[df['CNT'].astype(str).str.contains('Brazil|BRA', case=False, na=False)].copy()
            print(f"      - Brazil rows found: {len(df)}")
            
            if len(df) == 0: print("[ERROR] No Brazil rows found."); return

            # --- 2022 LOGIC: TEXT MATCHING ---
            def get_region_2022(stratum):
                s = str(stratum).upper()
                if 'CENTRO-OESTE' in s or 'CENTRO OESTE' in s: return 'Center-West'
                if 'NORDESTE' in s: return 'Northeast'
                if 'SUDESTE' in s: return 'Southeast'
                if 'NORTE' in s: return 'North'
                if 'SUL' in s: return 'South'
                return 'UNKNOWN'

            print("[INFO] Aggregating by Macro-Region (Text Match)...")
            df['Region'] = df['STRATUM'].apply(get_region_2022)
            df = df[df['Region'] != 'UNKNOWN']
            
            means = df.groupby('Region')[['PV1MATH', 'PV1READ', 'PV1SCIE']].mean().reset_index()
            counts = df['Region'].value_counts().reset_index()
            counts.columns = ['Region', 'Student_Count']
            
            res = pd.merge(counts, means, on='Region')
            res['Cognitive_Global_Mean'] = (res['PV1MATH'] + res['PV1READ'] + res['PV1SCIE']) / 3
            res = res.rename(columns={'PV1MATH': 'Math_Mean', 'PV1READ': 'Read_Mean', 'PV1SCIE': 'Science_Mean'})
            
            df_final = res.round(2).sort_values('Cognitive_Global_Mean', ascending=False)
            
            csv_path = CSV_OUT_DIR / 'pisa_2022_regional_summary.csv'
            xlsx_path = XLSX_OUT_DIR / 'pisa_2022_regional_summary.xlsx'
            df_final.to_csv(csv_path, index=False)
            df_final.to_excel(xlsx_path, index=False)
            print(f"[SUCCESS] Saved: {csv_path.name}")

        except Exception as e:
            print(f"[ERROR] {e}")

# --- 4. EXECUTION FLOW ---

def main():
    print("="*60)
    print("      COGNITIVE CAPITAL - PISA ETL (v7.1 Fix)")
    print("="*60)
    
    # 1. CLI or Input
    if len(sys.argv) > 1:
        choice = sys.argv[1]
    else:
        print("Options: [2015, 2018, 2022]")
        choice = input("Select Year or press ENTER for ALL: ").strip().lower()

    # 2. Resolve Targets (Logic: Empty input = Process ALL immediately)
    etl = PisaUnifiedETL()
    targets = []
    
    if not choice or choice in ['all', 'todos']:
        print("\n[AUTO] Default selection: ALL YEARS. Starting now...")
        targets = ['2015', '2018', '2022']
    elif choice == '2015': targets = ['2015']
    elif choice == '2018': targets = ['2018']
    elif choice == '2022': targets = ['2022']
    else:
        print("[ERROR] Invalid selection."); sys.exit(1)

    # 3. Execute
    for year in targets:
        with ExecutionTimer():
            if year == '2015': etl.run_2015()
            elif year == '2018': etl.run_2018()
            elif year == '2022': etl.run_2022()

if __name__ == "__main__":
    main()