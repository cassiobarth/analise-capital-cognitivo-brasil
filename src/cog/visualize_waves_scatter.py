"""
================================================================================
PROJECT:       COGNITIVE CAPITAL ANALYSIS - BRAZIL
SCRIPT:        src/cog/visualize_waves_scatter.py
DESCRIPTION:   Gera Scatter Plots (Gráficos de Dispersão) para as 3 Ondas.
               Visualiza a correlação entre Avaliação Internacional (PISA)
               e Avaliações Nacionais (ENEM/SAEB).
OUTPUT:        reports/varcog/graficos/scatter_wave_{YEAR}_{SOURCE}.png
================================================================================
"""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

# --- CONFIGURAÇÃO ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_PROC = PROJECT_ROOT / 'data' / 'processed'
REPORTS_XLSX = PROJECT_ROOT / 'reports' / 'varcog' / 'xlsx'
IMG_DIR = PROJECT_ROOT / 'reports' / 'varcog' / 'graficos'
IMG_DIR.mkdir(parents=True, exist_ok=True)

# Mapeamento UF -> Região (Para agregação 2018/22)
UF_TO_REGION = {
    'AC':'North', 'AL':'Northeast', 'AP':'North', 'AM':'North', 'BA':'Northeast', 'CE':'Northeast', 
    'DF':'Center-West', 'ES':'Southeast', 'GO':'Center-West', 'MA':'Northeast', 'MT':'Center-West', 
    'MS':'Center-West', 'MG':'Southeast', 'PA':'North', 'PB':'Northeast', 'PR':'South', 
    'PE':'Northeast', 'PI':'Northeast', 'RJ':'Southeast', 'RN':'Northeast', 'RS':'South', 
    'RO':'North', 'RR':'North', 'SC':'South', 'SP':'Southeast', 'SE':'Northeast', 'TO':'North'
}

# Configuração Visual
sns.set_style("whitegrid")
plt.rcParams.update({'font.size': 12})

def get_data_for_wave(year):
    """Prepara o DataFrame consolidado para o ano específico."""
    
    # 1. Carregar PISA
    if year == '2015':
        pisa = pd.read_csv(DATA_PROC / 'pisa_2015_states.csv')
        pisa = pisa.rename(columns={'UF': 'KEY', 'Cognitive_Global_Mean': 'PISA_Score'})
        granularity = 'State'
    else:
        # 2018 e 2022 são Regionais
        pisa = pd.read_csv(DATA_PROC / f'pisa_{year}_regional_summary.csv')
        pisa = pisa.rename(columns={'Region': 'KEY', 'Cognitive_Global_Mean': 'PISA_Score'})
        granularity = 'Region'

    # 2. Carregar ENEM (Sempre Estadual, precisa agregar se for Regional)
    try:
        enem_path = DATA_PROC / f'enem_table_{year}_3EM.csv'
        enem = pd.read_csv(enem_path)
        enem = enem.rename(columns={'SG_UF_PROVA': 'KEY', 'Enem_Global_Mean': 'ENEM_Score'})
        
        if granularity == 'Region':
            enem['KEY'] = enem['KEY'].map(UF_TO_REGION)
            enem = enem.groupby('KEY')[['ENEM_Score']].mean().reset_index()
    except:
        print(f"[AVISO] ENEM {year} não encontrado.")
        enem = None

    # 3. Carregar SAEB
    try:
        # Lógica de ano aproximado para SAEB
        saeb_year = '2015' if year == '2015' else ('2017' if year == '2018' else '2023')
        saeb_path = REPORTS_XLSX / f'saeb_table_{saeb_year}_3EM.xlsx'
        saeb = pd.read_excel(saeb_path)
        
        # Ajuste de coluna nota SAEB (tenta achar a média)
        if 'MEDIA_MT_LP' not in saeb.columns:
            if 'MEDIA_MT' in saeb.columns: saeb['MEDIA_MT_LP'] = (saeb['MEDIA_MT'] + saeb['MEDIA_LP']) / 2
        
        saeb = saeb.rename(columns={'UF': 'KEY', 'MEDIA_MT_LP': 'SAEB_Score'})
        
        if granularity == 'Region':
            saeb['KEY'] = saeb['KEY'].map(UF_TO_REGION)
            saeb = saeb.groupby('KEY')[['SAEB_Score']].mean().reset_index()
            
    except:
        print(f"[AVISO] SAEB {saeb_year} não encontrado.")
        saeb = None

    # Merge Final
    df = pisa[['KEY', 'PISA_Score']].copy()
    if enem is not None:
        df = pd.merge(df, enem[['KEY', 'ENEM_Score']], on='KEY', how='inner')
    if saeb is not None:
        df = pd.merge(df, saeb[['KEY', 'SAEB_Score']], on='KEY', how='inner')
        
    return df, granularity

def plot_scatter(df, x_col, y_col, label_col, title, filename):
    """Gera e salva o gráfico."""
    plt.figure(figsize=(10, 7))
    
    # Scatter Plot com Regressão
    sns.regplot(data=df, x=x_col, y=y_col, ci=95, scatter_kws={'s': 100, 'alpha':0.7}, line_kws={'color':'red'})
    
    # Labels nos pontos (Ex: SP, RJ ou South, North)
    for i in range(df.shape[0]):
        plt.text(
            df[x_col].iloc[i]+0.5, 
            df[y_col].iloc[i], 
            df[label_col].iloc[i], 
            fontsize=9,
            weight='bold'
        )

    # Correlação no Título
    corr = df[[x_col, y_col]].corr().iloc[0,1]
    
    plt.title(f"{title}\nCorrelação de Pearson (r): {corr:.3f}", fontsize=14)
    plt.xlabel(f"Pontuação Nacional ({x_col.split('_')[0]})")
    plt.ylabel("Pontuação Internacional (PISA)")
    
    plt.tight_layout()
    save_path = IMG_DIR / filename
    plt.savefig(save_path, dpi=300)
    plt.close()
    print(f"   [PLOT] Salvo: {save_path.name}")

def run_visuals():
    print("="*60)
    print("      GERADOR DE GRÁFICOS: ONDAS COGNITIVAS")
    print("="*60)
    
    waves = ['2015', '2018', '2022']
    
    for year in waves:
        print(f"\n--- Gerando Onda {year} ---")
        df, gran = get_data_for_wave(year)
        
        if df.empty:
            print(f"[SKIP] Sem dados consolidados para {year}")
            continue
            
        # Plot PISA x ENEM
        if 'ENEM_Score' in df.columns:
            plot_scatter(
                df, 'ENEM_Score', 'PISA_Score', 'KEY',
                f"Sincronia Cognitiva: PISA vs ENEM ({year})\nGranularidade: {gran}",
                f"scatter_wave_{year}_pisa_enem.png"
            )
            
        # Plot PISA x SAEB
        if 'SAEB_Score' in df.columns:
            plot_scatter(
                df, 'SAEB_Score', 'PISA_Score', 'KEY',
                f"Validação Sistêmica: PISA vs SAEB ({year})\nGranularidade: {gran}",
                f"scatter_wave_{year}_pisa_saeb.png"
            )

    print("\n" + "="*60)
    print(f"[SUCESSO] Gráficos salvos em: {IMG_DIR}")

if __name__ == "__main__":
    run_visuals()