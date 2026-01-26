"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/cog/06_visualize_correlations.py
RESEARCHERS: Dr. José Aparecido da Silva
             Me. Cássio Dalbem Barth
DATE:        2026-01-06

DESCRIPTION: 
    Generates advanced visualizations to explain the relationship between
    International (PISA) and National (ENEM) Cognitive Capital indicators.

    VISUALIZATIONS:
    1. SCATTER PLOT (Regression): Visual proof of the high correlation (r ~ 0.95).
    2. SLOPE CHART (Ranking): Visualizes the "Chair Swap" (Inversion) between South and Southeast.
    3. Z-SCORE BAR CHART: Compares relative performance magnitude on a standardized scale.

    INPUTS:
    - reports/varcog/csv/pisa_2022_regional_summary.csv
    - reports/varcog/xlsx/enem_consolidated_states_triennium.xlsx
"""
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats import zscore
from pathlib import Path

# --- CONFIGURATION ---
sns.set_theme(style="whitegrid")
plt.rcParams['figure.dpi'] = 300

# Paths
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent.parent
REPORT_DIR = PROJECT_ROOT / 'reports' / 'varcog'

FILE_PISA = REPORT_DIR / 'csv' / 'pisa_2022_regional_summary.csv'
FILE_ENEM = REPORT_DIR / 'xlsx' / 'enem_consolidated_states_triennium.xlsx'

OUTPUT_DIR = REPORT_DIR / 'graficos'
PATH_SCATTER = OUTPUT_DIR / 'viz_01_scatter_pisa_enem.png'
PATH_SLOPE   = OUTPUT_DIR / 'viz_02_slope_ranking_inversion.png'
PATH_ZSCORE  = OUTPUT_DIR / 'viz_03_zscore_magnitude.png'

def setup_directories():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def load_and_prep_data():
    print("[INFO] Loading and Aggregating Data...")
    
    # 1. PISA 2022 (Already Regional)
    if not FILE_PISA.exists():
        print(f"[ERROR] File not found: {FILE_PISA}")
        return None
    df_pisa = pd.read_csv(FILE_PISA)
    col_pisa = 'Cognitive_Global_Mean' if 'Cognitive_Global_Mean' in df_pisa.columns else 'Global_Mean'
    df_pisa = df_pisa[['Region', col_pisa]].rename(columns={col_pisa: 'PISA_Score'})

    # 2. ENEM Triennium (State -> Region Aggregate)
    if not FILE_ENEM.exists():
        print(f"[ERROR] File not found: {FILE_ENEM}")
        return None
    df_enem = pd.read_excel(FILE_ENEM)
    # Aggregate to Region Mean
    df_enem_reg = df_enem.groupby('Region')['Triennium_Mean'].mean().reset_index().rename(columns={'Triennium_Mean': 'ENEM_Score'})

    # 3. Merge
    df = pd.merge(df_pisa, df_enem_reg, on='Region')
    
    return df

def plot_scatter(df):
    print("[INFO] Generating Scatter Plot...")
    plt.figure(figsize=(8, 6))
    
    # Regression Plot
    # FIX: Changed 'linewidth' to 'linewidths' to avoid Matplotlib Alias Error
    sns.regplot(
        x='PISA_Score', 
        y='ENEM_Score', 
        data=df, 
        color='#2980b9', 
        scatter_kws={'s': 150, 'edgecolor': 'white', 'linewidths': 1.5},
        line_kws={'color': '#e74c3c', 'linestyle': '--'}
    )
    
    # Annotate Regions
    for i, row in df.iterrows():
        plt.text(
            row['PISA_Score'] + 1.5, 
            row['ENEM_Score'], 
            row['Region'], 
            fontsize=11, 
            fontweight='bold', 
            color='#34495e'
        )
        
    # Calculate Correlation for Title
    r = df['PISA_Score'].corr(df['ENEM_Score'])
    
    plt.title(f'Concurrent Validity: PISA vs ENEM (Region Level)\nPearson r = {r:.3f} (Very Strong)', fontsize=14, pad=15)
    plt.xlabel('PISA 2022 (International Standard)', fontsize=12)
    plt.ylabel('ENEM Triennium (National Standard)', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.5)
    
    plt.tight_layout()
    plt.savefig(PATH_SCATTER)
    plt.close()
    print(f"       - Saved: {PATH_SCATTER}")

def plot_slope_chart(df):
    print("[INFO] Generating Slope Chart...")
    
    # Calculate Ranks (1 = Best)
    df['Rank_PISA'] = df['PISA_Score'].rank(ascending=False)
    df['Rank_ENEM'] = df['ENEM_Score'].rank(ascending=False)
    
    fig, ax = plt.subplots(figsize=(8, 8))
    
    # Define vertical lines
    ax.vlines(x=1, ymin=1, ymax=5, color='black', alpha=0.1, linewidth=1)
    ax.vlines(x=3, ymin=1, ymax=5, color='black', alpha=0.1, linewidth=1)
    
    # Plot connecting lines
    for i, row in df.iterrows():
        # Choose color: Highlight the inversion (South/Southeast) vs Stability
        region = row['Region']
        if region in ['South', 'Southeast']:
            color = '#e74c3c' # Red for the swap
            weight = 'bold'
            alpha = 1.0
            width = 3
        else:
            color = '#95a5a6' # Grey for stable
            weight = 'normal'
            alpha = 0.6
            width = 1.5
            
        # Draw Line
        ax.plot([1, 3], [row['Rank_PISA'], row['Rank_ENEM']], marker='o', color=color, linewidth=width, alpha=alpha, markersize=10)
        
        # Annotate PISA side (Left)
        ax.text(0.9, row['Rank_PISA'], f"{region} ({int(row['Rank_PISA'])})", ha='right', va='center', fontsize=11, fontweight=weight, color=color)
        
        # Annotate ENEM side (Right)
        ax.text(3.1, row['Rank_ENEM'], f"{region} ({int(row['Rank_ENEM'])})", ha='left', va='center', fontsize=11, fontweight=weight, color=color)

    # Formatting
    ax.set_xticks([1, 3])
    ax.set_xticklabels(['PISA 2022\nRanking', 'ENEM Triennium\nRanking'], fontsize=12, fontweight='bold')
    
    # Invert Y axis so #1 is at top
    ax.set_ylim(5.5, 0.5)
    ax.set_yticks([]) # Hide Y numbers
    ax.set_ylabel('')
    
    ax.set_title('Hierarchical Stability Analysis: The "Chair Swap"', fontsize=14, pad=20)
    
    # Clean borders
    sns.despine(left=True, bottom=True, right=True, top=True)
    
    plt.tight_layout()
    plt.savefig(PATH_SLOPE)
    plt.close()
    print(f"       - Saved: {PATH_SLOPE}")

def plot_zscores(df):
    print("[INFO] Generating Z-Score Bar Chart...")
    
    # Calculate Z-Scores (Standardized)
    df['Z_PISA'] = zscore(df['PISA_Score'])
    df['Z_ENEM'] = zscore(df['ENEM_Score'])
    
    # Melt for Seaborn
    df_melt = df.melt(id_vars='Region', value_vars=['Z_PISA', 'Z_ENEM'], var_name='Indicator', value_name='Z_Score')
    df_melt['Indicator'] = df_melt['Indicator'].replace({'Z_PISA': 'PISA 2022', 'Z_ENEM': 'ENEM Triennium'})
    
    # Sort by Region order in PISA usually (South first)
    custom_order = df.sort_values('PISA_Score', ascending=False)['Region'].tolist()
    
    plt.figure(figsize=(10, 6))
    
    sns.barplot(
        x='Region', 
        y='Z_Score', 
        hue='Indicator', 
        data=df_melt, 
        order=custom_order,
        palette={'PISA 2022': '#2980b9', 'ENEM Triennium': '#27ae60'}
    )
    
    plt.axhline(0, color='black', linewidth=1)
    plt.title('Relative Magnitude (Z-Score): PISA vs ENEM', fontsize=14, pad=15)
    plt.ylabel('Standard Deviations from Mean (Z)', fontsize=12)
    plt.xlabel('')
    plt.legend(loc='upper right')
    
    plt.tight_layout()
    plt.savefig(PATH_ZSCORE)
    plt.close()
    print(f"       - Saved: {PATH_ZSCORE}")

if __name__ == "__main__":
    setup_directories()
    df_data = load_and_prep_data()
    
    if df_data is not None:
        plot_scatter(df_data)
        plot_slope_chart(df_data)
        plot_zscores(df_data)
        print("\n[SUCCESS] All visualizations generated in reports/varcog/graficos/")