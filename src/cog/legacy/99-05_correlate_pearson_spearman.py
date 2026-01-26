"""
PROJECT:     Cognitive Capital Analysis - Brazil
SCRIPT:      src/cog/05_correlate_pearson_spearman.py
DESCRIPTION: Calculates correlation matrices from the Master Longitudinal Panel.
"""
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
import numpy as np
from scipy.stats import pearsonr, spearmanr

# Caminhos
BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
REPORT_DIR = os.path.join(BASE_PATH, 'reports', 'varcog')
DATA_PROCESSED = os.path.join(BASE_PATH, 'data', 'processed')
os.makedirs(os.path.join(REPORT_DIR, 'csv'), exist_ok=True)
os.makedirs(os.path.join(REPORT_DIR, 'graficos'), exist_ok=True)

def calculate_pvalues(df, method='pearson'):
    df = df.dropna()
    dfcols = pd.DataFrame(columns=df.columns)
    pvalues = dfcols.transpose().join(dfcols, how='outer')
    for r in df.columns:
        for c in df.columns:
            if method == 'pearson':
                pvalues[r][c] = round(pearsonr(df[r], df[c])[1], 4)
            else:
                pvalues[r][c] = round(spearmanr(df[r], df[c])[1], 4)
    return pvalues

def main():
    print("[INFO] Starting Correlation Analysis...")
    
    # Busca inteligente do arquivo consolidado
    target_file = None
    # Prioridade 1: Arquivo gerado pelo passo 03
    p1 = os.path.join(DATA_PROCESSED, 'panel_longitudinal_waves.csv')
    # Prioridade 2: Arquivo consolidado regional (fonte alternativa)
    p2 = os.path.join(DATA_PROCESSED, 'Regional_Data_Source.csv')
    
    if os.path.exists(p1):
        target_file = p1
    elif os.path.exists(p2):
        target_file = p2
    else:
        # Fallback para busca recursiva se necessário
        print("[ERROR] Consolidate Data Panel not found.")
        return

    print(f"[INFO] Loading data from: {target_file}")
    df = pd.read_csv(target_file)
    
    # Identificar colunas numéricas de interesse
    # Remove colunas de metadados como Region, UF, Year se estiverem no índice
    if 'Region' in df.columns:
        df = df.set_index('Region')
    elif 'UF' in df.columns:
        df = df.set_index('UF')
        
    df_numeric = df.select_dtypes(include=[np.number])
    
    # Filtro de qualidade: Remover colunas com variância zero ou nulos excessivos
    df_numeric = df_numeric.dropna(axis=1, how='all')
    
    print(f"[INFO] Variables selected: {list(df_numeric.columns)}")
    
    if df_numeric.shape[1] < 2:
        print("[ERROR] Not enough variables for correlation.")
        return

    # 1. Pearson Correlation
    print("[ANALYTICS] Calculating Pearson Matrix...")
    pearson_corr = df_numeric.corr(method='pearson')
    pearson_p = calculate_pvalues(df_numeric, method='pearson')
    
    # Salvar outputs
    pearson_corr.to_csv(os.path.join(REPORT_DIR, 'csv', 'pearson_correlation.csv'))
    pearson_p.to_csv(os.path.join(REPORT_DIR, 'csv', 'pearson_pvalues.csv'))
    
    # 2. Spearman Correlation
    print("[ANALYTICS] Calculating Spearman Matrix...")
    spearman_corr = df_numeric.corr(method='spearman')
    spearman_corr.to_csv(os.path.join(REPORT_DIR, 'csv', 'spearman_correlation.csv'))
    
    # 3. Visualização Rápida
    plt.figure(figsize=(12, 10))
    sns.heatmap(pearson_corr, annot=True, fmt=".2f", cmap='coolwarm', vmin=-1, vmax=1)
    plt.title('Cognitive Capital Correlation Matrix (Pearson)')
    plt.tight_layout()
    plt.savefig(os.path.join(REPORT_DIR, 'graficos', 'correlation_heatmap.png'))
    print("[SUCCESS] Analysis complete. Reports saved in reports/varcog/")

if __name__ == "__main__":
    main()