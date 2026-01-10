"""
================================================================================
SCRIPT:        src/tests/debug_pisa_2018_extended.py
DESCRIPTION:   Mineração de Variáveis Contextuais no PISA 2018.
               Investiga a existência e qualidade de dados sobre:
               - Idade e Gênero
               - Série (Grade) e Repetência
               - Nível Socioeconômico (ESCS)
               - Background Familiar
================================================================================
"""

import pyreadstat
import pandas as pd
import numpy as np
import os
import sys

# Caminho do arquivo
BASE_DIR = os.getcwd()
FILE_PATH = os.path.join(BASE_DIR, 'data', 'raw', 'Pisa', 'pisa_2018', 'CY07_MSU_STU_QQQ.sav')

def mining_scholastic_vars():
    print("--- MINERAÇÃO DE VARIÁVEIS CONTEXTUAIS (PISA 2018) ---")
    
    if not os.path.exists(FILE_PATH):
        print(f"[ERRO] Arquivo não encontrado: {FILE_PATH}")
        return

    try:
        # 1. LER METADADOS
        print("[1/3] Lendo Dicionário de Variáveis (Metadados)...")
        _, meta = pyreadstat.read_sav(FILE_PATH, metadataonly=True)
        
        # Mapa de Labels: Nome da Coluna -> Descrição (Label)
        col_labels = dict(zip(meta.column_names, meta.column_labels))
        
        # 2. DEFINIR ALVOS DE BUSCA
        # Vamos procurar códigos conhecidos do PISA ou palavras-chave nos labels
        targets = {
            'STUDENT_ID': ['CNTSTUID'],
            'AGE': ['AGE'],
            'GENDER': ['ST004D01T', 'GENDER'], # ST004 geralmente é genero
            'GRADE': ['ST001D01T', 'GRADE'],   # ST001 geralmente é a série atual
            'ESCS': ['ESCS'],                  # Índice Socioeconômico
            'REPEAT': ['REPEAT', 'ST127Q01TA', 'ST127Q02TA', 'ST127Q03TA'], # Repetência (Primário/Secundário)
            'PARENTS_EDU': ['HISCED', 'PARED'], # Maior escolaridade dos pais
            'IMMIGRATION': ['IMMIG']
        }
        
        found_cols = ['CNT'] # Sempre trazer o país
        
        print("\n[2/3] Identificando colunas disponíveis...")
        for category, candidates in targets.items():
            found_for_cat = []
            for c in candidates:
                if c in meta.column_names:
                    found_for_cat.append(c)
            
            if found_for_cat:
                print(f"   > {category}: Encontrado {found_for_cat}")
                found_cols.extend(found_for_cat)
            else:
                print(f"   X {category}: Não encontrado pelos códigos padrão.")

        # Remove duplicatas
        found_cols = list(set(found_cols))

        # 3. CARREGAR AMOSTRA BRASIL
        print(f"\n[3/3] Carregando dados para {len(found_cols)} variáveis (Filtro: BRA)...")
        df, _ = pyreadstat.read_sav(FILE_PATH, usecols=found_cols)
        
        # Filtrar Brasil
        df_bra = df[df['CNT'].astype(str).str.contains('BRA|Brazil|76', case=False, na=False)].copy()
        
        if df_bra.empty:
            print("[ERRO] Sem dados para Brasil.")
            return

        print(f"\n=== RELATÓRIO DE DADOS ESCOLARES (N={len(df_bra)}) ===")
        
        # --- ANÁLISE IDADE ---
        if 'AGE' in df_bra.columns:
            print("\n1. IDADE DO ALUNO (AGE):")
            print(df_bra['AGE'].describe().round(2))
        
        # --- ANÁLISE SÉRIE (GRADE) ---
        # Tenta achar a coluna de grade encontrada
        grade_col = next((c for c in ['ST001D01T', 'GRADE'] if c in df_bra.columns), None)
        if grade_col:
            print(f"\n2. SÉRIE ATUAL ({grade_col}):")
            # Labels podem ajudar a entender o código (ex: 7 = 7º ano, 10 = 1º ano EM)
            if grade_col in meta.variable_value_labels:
                labels = meta.variable_value_labels[grade_col]
                print(f"   Labels: {labels}")
            print(df_bra[grade_col].value_counts().sort_index())
            
        # --- ANÁLISE SOCIOECONÔMICA ---
        if 'ESCS' in df_bra.columns:
            print("\n3. NÍVEL SOCIOECONÔMICO (ESCS - Index):")
            print("   (Média 0 = Média OCDE. Negativo = Abaixo)")
            print(df_bra['ESCS'].describe().round(2))
            
        # --- ANÁLISE REPETÊNCIA ---
        # PISA tem variaveis sobre "Repetiu Primário?", "Repetiu Secundário?"
        rep_cols = [c for c in df_bra.columns if 'ST127' in c] # ST127 costuma ser "Have you ever repeated"
        if rep_cols:
            print("\n4. HISTÓRICO DE REPETÊNCIA (ST127 - Já repetiu?):")
            for rc in rep_cols:
                label = col_labels.get(rc, rc)
                print(f"   Var: {rc} ({label[:40]}...)")
                # Mapear labels se houver
                if rc in meta.variable_value_labels:
                    vals = df_bra[rc].map(meta.variable_value_labels[rc])
                    print(vals.value_counts(normalize=True).round(3))
                else:
                    print(df_bra[rc].value_counts(normalize=True).round(3))

        # --- ANÁLISE PAIS ---
        if 'HISCED' in df_bra.columns:
             print("\n5. ESCOLARIDADE DOS PAIS (HISCED):")
             # Highest International Social and Economic Status Index
             if 'HISCED' in meta.variable_value_labels:
                 print(df_bra['HISCED'].map(meta.variable_value_labels['HISCED']).value_counts())
             else:
                 print(df_bra['HISCED'].value_counts().sort_index())

    except Exception as e:
        print(f"[CRITICAL FAIL] {e}")

if __name__ == "__main__":
    mining_scholastic_vars()