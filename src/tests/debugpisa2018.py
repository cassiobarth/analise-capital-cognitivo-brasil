"""
================================================================================
SCRIPT:        src/tests/debug_pisa_2018_light.py
DESCRIPTION:   Memory-efficient inspection of PISA 2018 raw file structure.
               Uses pyreadstat to avoid loading the full dataset into RAM.
================================================================================
"""

import pyreadstat
import pandas as pd
import os
import sys

# Caminho do arquivo
BASE_DIR = os.getcwd()
FILE_PATH = os.path.join(BASE_DIR, 'data', 'raw', 'Pisa', 'pisa_2018', 'CY07_MSU_STU_QQQ.sav')

def investigate_memory_safe():
    print("--- INVESTIGAÇÃO CIRÚRGICA PISA 2018 (LOW MEMORY) ---")
    
    if not os.path.exists(FILE_PATH):
        print(f"[ERRO] Arquivo não encontrado: {FILE_PATH}")
        return

    try:
        # 1. LER APENAS METADADOS (Instantâneo, gasta ~0 RAM)
        print("[INFO] Lendo cabeçalho (Metadados)...")
        _, meta = pyreadstat.read_sav(FILE_PATH, metadataonly=True)
        
        all_cols = meta.column_names
        print(f"[STATS] Total de colunas disponíveis: {len(all_cols)}")
        
        # Verificar existência das colunas chaves
        targets = ['CNT', 'SUBNATIO', 'STRATUM', 'REGION']
        found = [c for c in targets if c in all_cols]
        print(f"[CHECK] Colunas alvo encontradas: {found}")

        if 'CNT' not in found:
            print("[CRITICO] Coluna CNT não encontrada. Impossível filtrar país.")
            return

        # 2. LER APENAS AS COLUNAS ALVO (Reduz o uso de RAM em 99%)
        print("\n[INFO] Carregando APENAS colunas geográficas para filtrar Brasil...")
        # Lemos apenas as colunas que importam. Isso deve ocupar poucos MBs.
        df, _ = pyreadstat.read_sav(FILE_PATH, usecols=found)
        
        # 3. FILTRAR BRASIL
        print("[PROCESS] Filtrando linhas do Brasil...")
        # Regex para garantir que pegamos BRA, Brazil ou 076
        mask = df['CNT'].astype(str).str.contains('BRA|Brazil|76', case=False, na=False)
        df_bra = df[mask]
        
        if df_bra.empty:
            print("[ALERTA] Nenhuma linha encontrada para o Brasil.")
            return
            
        print(f"[DADOS] {len(df_bra)} estudantes brasileiros encontrados.")
        
        # 4. EXIBIR VALORES ÚNICOS (A Prova Real)
        print("\n" + "="*50)
        print("RELATÓRIO DE GRANULARIDADE GEOGRÁFICA")
        print("="*50)
        
        if 'SUBNATIO' in df_bra.columns:
            subs = df_bra['SUBNATIO'].unique()
            print(f"\n>>> COLUNA: SUBNATIO")
            print(f"    Qtd Valores Únicos: {len(subs)}")
            print(f"    Valores: {subs}")
            if len(subs) == 1 and '760000' in str(subs[0]):
                print("    [CONCLUSÃO] SUBNATIO contém apenas o código do PAÍS (Anonimizado).")
            else:
                print("    [CONCLUSÃO] SUBNATIO parece conter estados!")

        if 'STRATUM' in df_bra.columns:
            strats = df_bra['STRATUM'].unique()
            print(f"\n>>> COLUNA: STRATUM")
            print(f"    Qtd Valores Únicos: {len(strats)}")
            print(f"    Amostra (5 primeiros): {strats[:5]}")
            
            # Teste rápido de lógica de Região
            sample = str(strats[0])
            if sample.startswith('BRA') and sample[3:5].isdigit():
                print("    [CONCLUSÃO] STRATUM segue padrão 'BRA' + Dígitos (Permite extrair Região).")
            else:
                print("    [CONCLUSÃO] STRATUM segue padrão textual/misto.")

    except Exception as e:
        print(f"[CRITICAL FAIL] Erro de execução: {e}")

if __name__ == "__main__":
    investigate_memory_safe()