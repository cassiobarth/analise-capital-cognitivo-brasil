"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/cog/05_correlate_pearson_spearman.py
RESEARCHERS: Dr. José Aparecido da Silva
             Me. Cássio Dalbem Barth
DATE:        2026-01-06

DESCRIPTION: 
    Performs Full Correlation Analysis between International (PISA) and 
    National (SAEB, ENEM) indicators.
    
    METHODOLOGY:
    1. Aggregates National Data to MACRO-REGION level (N=5).
    2. Calculates two types of Correlation:
       - PEARSON (r): Measures Linear Predictive Power (Magnitude).
       - SPEARMAN (rho): Measures Rank Stability (Hierarchy).

    INPUTS (Required in reports/varcog/):
    1. pisa_2018_regional_summary.csv   (Historical Anchor)
    2. pisa_2022_regional_summary.csv   (Current Standard)
    3. saeb_2023_states.csv             (National Census)
    4. enem_consolidated_states_triennium.xlsx (Robust 3-Year Indicator)

    EXPORTS:
    1. reports/varcog/xlsx/correlation_matrix_full.xlsx
    2. reports/varcog/graficos/predictive_power_timeline.png
"""
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

# --- CONFIGURATION ---
sns.set_theme(style="whitegrid")

# Paths
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent.parent
REPORT_DIR = PROJECT_ROOT / 'reports' / 'varcog'

# Inputs
FILE_PISA_18 = REPORT_DIR / 'csv' / 'pisa_2018_regional_summary.csv'
FILE_PISA_22 = REPORT_DIR / 'csv' / 'pisa_2022_regional_summary.csv'
FILE_SAEB    = REPORT_DIR / 'csv' / 'saeb_2023_states.csv'
FILE_ENEM    = REPORT_DIR / 'xlsx' / 'enem_consolidated_states_triennium.xlsx'

# Outputs
PATH_CORR_XLSX = REPORT_DIR / 'xlsx' / 'correlation_matrix_full.xlsx'
PATH_LAG_IMG   = REPORT_DIR / 'graficos' / 'predictive_power_timeline.png'

def setup_directories():
    for p in [PATH_CORR_XLSX.parent, PATH_LAG_IMG.parent]:
        p.mkdir(parents=True, exist_ok=True)

def load_data():
    print("[INFO] Loading datasets...")

    # 1. PISA 2018
    if not FILE_PISA_18.exists():
        print(f"[ERROR] PISA 2018 file not found: {FILE_PISA_18}")
        return None
    df_p18 = pd.read_csv(FILE_PISA_18)
    col_18 = 'Cognitive_Global_Mean' if 'Cognitive_Global_Mean' in df_p18.columns else 'Global_Mean'
    df_p18 = df_p18[['Region', col_18]].rename(columns={col_18: 'PISA_2018'})

    # 2. PISA 2022
    if not FILE_PISA_22.exists(): 
        print(f"[ERROR] PISA 2022 file not found: {FILE_PISA_22}")
        return None
    df_p22 = pd.read_csv(FILE_PISA_22)
    col_22 = 'Cognitive_Global_Mean' if 'Cognitive_Global_Mean' in df_p22.columns else 'Global_Mean'
    df_p22 = df_p22[['Region', col_22]].rename(columns={col_22: 'PISA_2022'})

    # 3. SAEB 2023
    if not FILE_SAEB.exists(): 
        print(f"[ERROR] SAEB 2023 file not found: {FILE_SAEB}")
        return None
    df_saeb = pd.read_csv(FILE_SAEB)
    df_saeb_reg = df_saeb.groupby('Region')['SAEB_General'].mean().reset_index().rename(columns={'SAEB_General': 'SAEB_2023'})

    # 4. ENEM Triennium
    if not FILE_ENEM.exists(): 
        print(f"[ERROR] ENEM Triennium file not found: {FILE_ENEM}")
        return None
    df_enem = pd.read_excel(FILE_ENEM)
    
    # Aggregate to Region
    df_enem_reg = df_enem.groupby('Region')[['Mean_2022', 'Mean_2023', 'Mean_2024', 'Triennium_Mean']].mean().reset_index()
    df_enem_reg = df_enem_reg.rename(columns={
        'Mean_2022': 'ENEM_2022',
        'Mean_2023': 'ENEM_2023',
        'Mean_2024': 'ENEM_2024',
        'Triennium_Mean': 'ENEM_Triennium'
    })

    # --- MERGE ---
    print("[INFO] Merging all indicators by Macro-Region...")
    df_final = pd.merge(df_p18, df_p22, on='Region')
    df_final = pd.merge(df_final, df_saeb_reg, on='Region')
    df_final = pd.merge(df_final, df_enem_reg, on='Region')
    
    return df_final

def analyze_correlations(df):
    print("\n[INFO] Calculating Correlation Matrices...")
    
    cols = ['PISA_2018', 'PISA_2022', 'SAEB_2023', 'ENEM_2022', 'ENEM_2023', 'ENEM_2024']
    
    # 1. PEARSON
    pearson = df[cols].corr(method='pearson')
    print("\n--- PEARSON (Predictive Power) ---")
    print(pearson.round(4))
    
    # 2. SPEARMAN
    spearman = df[cols].corr(method='spearman')
    print("\n--- SPEARMAN (Rank Hierarchy) ---")
    print(spearman.round(4))
    
    # Save to Excel
    try:
        with pd.ExcelWriter(PATH_CORR_XLSX) as writer:
            pearson.to_excel(writer, sheet_name='Pearson')
            spearman.to_excel(writer, sheet_name='Spearman')
            df.round(2).to_excel(writer, sheet_name='Regional_Data_Source', index=False)
        print(f"\n[SUCCESS] Excel report saved: {PATH_CORR_XLSX}")
    except Exception as e:
        print(f"[ERROR] Could not save Excel: {e}")
    
    return pearson

def plot_predictive_power(corr_matrix):
    print("[INFO] Generating Predictive Power Chart...")
    years = ['ENEM_2022', 'ENEM_2023', 'ENEM_2024']
    
    # Extract correlations
    pisa_18_corrs = [corr_matrix.loc['PISA_2018', y] for y in years]
    pisa_22_corrs = [corr_matrix.loc['PISA_2022', y] for y in years]
    
    plt.figure(figsize=(10, 6))
    
    x_indexes = np.arange(len(years))
    
    # Plot PISA 2018 (RED)
    plt.plot(x_indexes, pisa_18_corrs, marker='o', linewidth=3, color='#e74c3c', label='Predictor: PISA 2018')
    
    # Plot PISA 2022 (BLUE)
    plt.plot(x_indexes, pisa_22_corrs, marker='o', linewidth=3, color='#2980b9', label='Predictor: PISA 2022')
    
    plt.xticks(x_indexes, years)
    
    # --- DYNAMIC Y-LIMITS (The Fix) ---
    # We find the min and max of all data points to set the limits automatically
    all_values = pisa_18_corrs + pisa_22_corrs
    min_val = min(all_values)
    max_val = max(all_values)
    
    # Add a 5% margin
    plt.ylim(min_val - 0.05, max_val + 0.02)
    
    plt.title("Predictive Power of PISA on Subsequent ENEM Scores (Pearson r)", fontsize=14, weight='bold')
    plt.ylabel("Correlation (r)", fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend(fontsize=11)
    
    # Annotate Values for PISA 2022 (BLUE)
    for i, val in enumerate(pisa_22_corrs):
        plt.annotate(f"{val:.3f}", xy=(float(i), val), xytext=(0, 10), textcoords='offset points', color='#2980b9', weight='bold')

    # Annotate Values for PISA 2018 (RED) - ADDED
    for i, val in enumerate(pisa_18_corrs):
        plt.annotate(f"{val:.3f}", xy=(float(i), val), xytext=(0, -15), textcoords='offset points', color='#e74c3c', weight='bold')

    plt.tight_layout()
    plt.savefig(PATH_LAG_IMG, dpi=300)
    plt.close()
    print(f"       - Chart saved: {PATH_LAG_IMG}")

if __name__ == "__main__":
    setup_directories()
    df_data = load_data()
    
    if df_data is not None:
        matrix = analyze_correlations(df_data)
        plot_predictive_power(matrix)