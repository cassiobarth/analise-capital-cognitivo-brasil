"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/cog/06_analyze_2015_snapshot.py
RESEARCHERS: Dr. Jose Aparecido da Silva
             Me. Cassio Dalbem Barth
DATE:        2026-01-08

DESCRIPTION:
    Performs the 'Golden Year' analysis (2015), triangulating data from
    three independent sources: PISA, ENEM, and SAEB.
    
    GOAL:
    Verify if the Cognitive Capital measurements are consistent across 
    different instruments (International, National Exam, National Assessment).

INPUT:
    - data/processed/pisa_2015_states.csv
    - data/processed/enem_table_2015.csv
    - data/processed/saeb_table_2015.csv

OUTPUT:
    - reports/varcog/csv/triangulation_2015_matrix.csv
    - reports/varcog/graficos/triangulation_2015_heatmap.png
"""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
import sys

# --- CONFIGURATION ---
BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_PATH, 'data', 'processed')
REPORT_CSV = os.path.join(BASE_PATH, 'reports', 'varcog', 'csv')
REPORT_IMG = os.path.join(BASE_PATH, 'reports', 'varcog', 'graficos')

os.makedirs(REPORT_CSV, exist_ok=True)
os.makedirs(REPORT_IMG, exist_ok=True)

def load_dataset(filename, prefix):
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        print(f"[ERROR] Missing file: {filename}")
        return None
    
    df = pd.read_csv(path)
    # Keep only useful columns to avoid clutter
    # Assuming standard columns from previous scripts
    return df

def main():
    print("="*60)
    print("[START] 2015 Data Triangulation (PISA x ENEM x SAEB)")
    print("="*60)

    # 1. Load Data
    # PISA 2015
    df_pisa = load_dataset('pisa_2015_states.csv', 'PISA')
    if df_pisa is not None:
        # Standardize PISA columns for merge
        cols_pisa = ['UF', 'Cognitive_Global_Mean', 'Math', 'Read', 'Science']
        df_pisa = df_pisa[cols_pisa].rename(columns={
            'Cognitive_Global_Mean': 'PISA_Global',
            'Math': 'PISA_Math',
            'Read': 'PISA_Read',
            'Science': 'PISA_Science'
        })

    # ENEM 2015
    df_enem = load_dataset('enem_table_2015.csv', 'ENEM')
    if df_enem is not None:
        # Columns: Region, UF, Mean_General, Math, Language...
        cols_enem = ['UF', 'Mean_General', 'Math', 'Language', 'Natural_Sciences']
        df_enem = df_enem[cols_enem].rename(columns={
            'Mean_General': 'ENEM_General',
            'Math': 'ENEM_Math',
            'Language': 'ENEM_Lang',
            'Natural_Sciences': 'ENEM_Nature'
        })

    # SAEB 2015
    df_saeb = load_dataset('saeb_table_2015.csv', 'SAEB')
    if df_saeb is not None:
        # Columns: Region, UF, SAEB_General, Math_Mean, Language_Mean
        cols_saeb = ['UF', 'SAEB_General', 'Math_Mean', 'Language_Mean']
        df_saeb = df_saeb[cols_saeb].rename(columns={
            'SAEB_General': 'SAEB_General',
            'Math_Mean': 'SAEB_Math',
            'Language_Mean': 'SAEB_Lang'
        })

    # 2. Merge (Inner Join to keep only states present in all)
    if any(x is None for x in [df_pisa, df_enem, df_saeb]):
        print("[CRITICAL] Cannot proceed due to missing files.")
        return

    print("[INFO] Merging datasets...")
    merged = df_pisa.merge(df_enem, on='UF').merge(df_saeb, on='UF')
    print(f"       - States matched: {len(merged)}")
    print(f"       - Columns: {list(merged.columns)}")

    # 3. Correlation Analysis
    # We focus on the numeric columns
    numeric_cols = [c for c in merged.columns if c != 'UF']
    corr_matrix = merged[numeric_cols].corr(method='pearson')

    # Save Matrix
    out_csv = os.path.join(REPORT_CSV, 'triangulation_2015_matrix.csv')
    corr_matrix.to_csv(out_csv)
    print(f"\n[OUTPUT] Matrix saved: {out_csv}")

    # 4. Specific Insights
    print("\n[INSIGHTS] Key Correlations (Pearson r):")
    
    # Global Comparisons
    r_pisa_enem = corr_matrix.loc['PISA_Global', 'ENEM_General']
    r_pisa_saeb = corr_matrix.loc['PISA_Global', 'SAEB_General']
    r_enem_saeb = corr_matrix.loc['ENEM_General', 'SAEB_General']
    
    print(f"   > Global Consistency:")
    print(f"     - PISA x ENEM: {r_pisa_enem:.3f}")
    print(f"     - PISA x SAEB: {r_pisa_saeb:.3f}")
    print(f"     - ENEM x SAEB: {r_enem_saeb:.3f}")

    # Domain Comparisons (Math)
    r_math_pisa_enem = corr_matrix.loc['PISA_Math', 'ENEM_Math']
    r_math_pisa_saeb = corr_matrix.loc['PISA_Math', 'SAEB_Math']
    print(f"   > Math Domain:")
    print(f"     - PISA Math x ENEM Math: {r_math_pisa_enem:.3f}")
    print(f"     - PISA Math x SAEB Math: {r_math_pisa_saeb:.3f}")

    # 5. Visualization (Heatmap)
    plt.figure(figsize=(10, 8))
    sns.heatmap(corr_matrix, annot=True, fmt=".2f", cmap='RdBu_r', vmin=0, vmax=1)
    plt.title('2015 Cognitive Capital Triangulation\n(PISA - ENEM - SAEB)', fontsize=14)
    plt.tight_layout()
    
    out_img = os.path.join(REPORT_IMG, 'triangulation_2015_heatmap.png')
    plt.savefig(out_img, dpi=300)
    print(f"\n[OUTPUT] Heatmap saved: {out_img}")

    # 6. Save Merged Data for JASP
    # This is the file you will open in JASP later!
    out_jasp = os.path.join(REPORT_CSV, 'dataset_for_jasp_2015.csv')
    merged.to_csv(out_jasp, index=False)
    print(f"       - JASP Dataset: {out_jasp}")

if __name__ == "__main__":
    main()