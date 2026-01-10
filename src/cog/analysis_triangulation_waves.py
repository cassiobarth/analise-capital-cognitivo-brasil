"""
================================================================================
PROJECT:       COGNITIVE CAPITAL ANALYSIS - BRAZIL
SCRIPT:        src/cog/analysis_triangulation_waves.py
DESCRIPTION:   Realiza a Triangulação de Dados (PISA x ENEM x SAEB) por ondas.
               - Onda 1 (2015): Correlação por ESTADO (UF).
               - Onda 2 (2018) e 3 (2022): Agrega ENEM/SAEB e correlaciona por REGIÃO.
OUTPUT:        reports/varcog/xlsx/triangulation_waves_consolidated.xlsx
================================================================================
"""

import pandas as pd
import numpy as np
import os
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

# --- CONFIGURAÇÃO ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_PROC = PROJECT_ROOT / 'data' / 'processed'
REPORTS_XLSX = PROJECT_ROOT / 'reports' / 'varcog' / 'xlsx'
IMG_DIR = PROJECT_ROOT / 'reports' / 'varcog' / 'graficos'

# Mapeamento de Arquivos (Baseado no seu ls -R)
# Ajuste os nomes se necessário, estou usando os padrões mais prováveis da sua lista
FILES_MAP = {
    '2015': {
        'PISA': {'path': DATA_PROC / 'pisa_2015_states.csv', 'col_join': 'UF', 'col_score': 'Cognitive_Global_Mean'},
        'ENEM': {'path': DATA_PROC / 'enem_table_2015_3EM.csv', 'col_join': 'SG_UF_PROVA', 'col_score': 'Enem_Global_Mean'},
        'SAEB': {'path': REPORTS_XLSX / 'saeb_table_2015_3EM.xlsx', 'col_join': 'UF', 'col_score': 'MEDIA_MT_LP'} 
    },
    '2018': {
        'PISA': {'path': DATA_PROC / 'pisa_2018_regional_summary.csv', 'col_join': 'Region', 'col_score': 'Cognitive_Global_Mean'},
        'ENEM': {'path': DATA_PROC / 'enem_table_2018_3EM.csv', 'col_join': 'SG_UF_PROVA', 'col_score': 'Enem_Global_Mean'},
        'SAEB': {'path': REPORTS_XLSX / 'saeb_table_2017_3EM.xlsx', 'col_join': 'UF', 'col_score': 'MEDIA_MT_LP'}
    },
    '2022': {
        'PISA': {'path': DATA_PROC / 'pisa_2022_regional_summary.csv', 'col_join': 'Region', 'col_score': 'Cognitive_Global_Mean'},
        'ENEM': {'path': DATA_PROC / 'enem_table_2022_3EM.csv', 'col_join': 'SG_UF_PROVA', 'col_score': 'Enem_Global_Mean'},
        'SAEB': {'path': REPORTS_XLSX / 'saeb_table_2023_3EM.xlsx', 'col_join': 'UF', 'col_score': 'MEDIA_MT_LP'}
    }
}

# Dicionário Auxiliar UF -> Região (Para agrupar ENEM/SAEB nas ondas 2 e 3)
UF_TO_REGION = {
    'AC':'North', 'AL':'Northeast', 'AP':'North', 'AM':'North', 'BA':'Northeast', 'CE':'Northeast', 
    'DF':'Center-West', 'ES':'Southeast', 'GO':'Center-West', 'MA':'Northeast', 'MT':'Center-West', 
    'MS':'Center-West', 'MG':'Southeast', 'PA':'North', 'PB':'Northeast', 'PR':'South', 
    'PE':'Northeast', 'PI':'Northeast', 'RJ':'Southeast', 'RN':'Northeast', 'RS':'South', 
    'RO':'North', 'RR':'North', 'SC':'South', 'SP':'Southeast', 'SE':'Northeast', 'TO':'North'
}

def load_file(path):
    """Lê CSV ou Excel automaticamente."""
    if not path.exists():
        return None
    if path.suffix == '.csv':
        return pd.read_csv(path)
    elif path.suffix == '.xlsx':
        return pd.read_excel(path)
    return None

def normalize_cols(df, key_col, score_col, prefix):
    """Padroniza nomes para o merge."""
    # Renomeia a chave para 'KEY' e a nota para '{PREFIX}_Score'
    # Ex: UF -> KEY, Cognitive_Mean -> PISA_Score
    cols = {key_col: 'KEY', score_col: f'{prefix}_Score'}
    # Se tiver Grade, traz também
    if 'Grade' in df.columns: cols['Grade'] = f'{prefix}_Grade'
    
    return df.rename(columns=cols)[list(cols.values())]

def aggregate_to_region(df_state):
    """Converte dados estaduais (UF) para Regionais (Média)."""
    # 1. Recuperar UF da coluna KEY (se KEY for UF) ou usar mapa
    # Assumindo que o input tem 'KEY' como UF sigla (ex: SP, RJ)
    df = df_state.copy()
    df['Region'] = df['KEY'].map(UF_TO_REGION)
    
    # Agrupa e calcula média
    numeric_cols = [c for c in df.columns if 'Score' in c or 'Grade' in c]
    df_reg = df.groupby('Region')[numeric_cols].mean().reset_index()
    return df_reg.rename(columns={'Region': 'KEY'}) # Padroniza para KEY de novo

def run_triangulation():
    print("="*60)
    print("      TRIANGULAÇÃO: PISA x ENEM x SAEB (Ondas)")
    print("="*60)
    
    writer = pd.ExcelWriter(REPORTS_XLSX / 'triangulation_waves_consolidated.xlsx', engine='openpyxl')
    
    for wave, sources in FILES_MAP.items():
        print(f"\n--- Processando Onda {wave} ---")
        
        # 1. Carregar PISA (O Pivot da análise)
        pisa_raw = load_file(sources['PISA']['path'])
        if pisa_raw is None:
            print(f"[SKIP] PISA {wave} não encontrado.")
            continue
            
        # Determinar Nível (Estado ou Região?)
        is_regional = 'Region' in sources['PISA']['col_join'] and wave != '2015'
        print(f"   Modo: {'Regional (Agregado)' if is_regional else 'Estadual (Direto)'}")

        # Preparar PISA
        df_final = normalize_cols(pisa_raw, sources['PISA']['col_join'], sources['PISA']['col_score'], 'PISA')
        
        # 2. Carregar e Merge ENEM
        enem_raw = load_file(sources['ENEM']['path'])
        if enem_raw is not None:
            df_enem = normalize_cols(enem_raw, sources['ENEM']['col_join'], sources['ENEM']['col_score'], 'ENEM')
            
            if is_regional:
                print("   -> Agregando ENEM de UF para Região...")
                df_enem = aggregate_to_region(df_enem)
                
            df_final = pd.merge(df_final, df_enem, on='KEY', how='inner')
        else:
            print("   [AVISO] Arquivo ENEM não encontrado.")

        # 3. Carregar e Merge SAEB
        saeb_raw = load_file(sources['SAEB']['path'])
        if saeb_raw is not None:
            # SAEB muitas vezes tem coluna 'MEDIA_MT_LP' ou similar. Ajuste dinâmico se precisar.
            # Aqui assumimos que você vai garantir que o nome da coluna no dict FILES_MAP está certo.
            # Se não tiver MEDIA_MT_LP, tenta achar MT e LP e fazer média
            if sources['SAEB']['col_score'] not in saeb_raw.columns:
                if 'MEDIA_MT' in saeb_raw.columns and 'MEDIA_LP' in saeb_raw.columns:
                    saeb_raw['MEDIA_MT_LP'] = (saeb_raw['MEDIA_MT'] + saeb_raw['MEDIA_LP']) / 2
            
            df_saeb = normalize_cols(saeb_raw, sources['SAEB']['col_join'], sources['SAEB']['col_score'], 'SAEB')
            
            if is_regional:
                print("   -> Agregando SAEB de UF para Região...")
                df_saeb = aggregate_to_region(df_saeb)
                
            df_final = pd.merge(df_final, df_saeb, on='KEY', how='inner')
        else:
            print("   [AVISO] Arquivo SAEB não encontrado.")

        # 4. Correlação e Exportação
        print(f"   Dados Consolidados: {len(df_final)} linhas.")
        
        # Correlação
        cols_corr = [c for c in df_final.columns if 'Score' in c]
        if len(cols_corr) > 1:
            corr = df_final[cols_corr].corr()
            print("   Matriz de Correlação:")
            print(corr.round(3))
            
            # Salvar Dados Brutos e Correlação
            df_final.to_excel(writer, sheet_name=f'{wave}_Data', index=False)
            corr.to_excel(writer, sheet_name=f'{wave}_Corr')
            
            # Plot
            plt.figure(figsize=(6, 5))
            sns.heatmap(corr, annot=True, cmap='viridis', vmin=0, vmax=1)
            plt.title(f"Triangulação Onda {wave}")
            plt.tight_layout()
            plt.savefig(IMG_DIR / f'triangulation_{wave}_heatmap.png')
            plt.close()
        
    writer.close()
    print("\n" + "="*60)
    print(f"[SUCESSO] Relatório: {REPORTS_XLSX / 'triangulation_waves_consolidated.xlsx'}")

if __name__ == "__main__":
    run_triangulation()