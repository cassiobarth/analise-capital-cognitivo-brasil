"""
PROJETO: ANALISE DO REPERTORIO COGNITIVO NO BRASIL
ARQUIVO: 04_extrair_pisa_final_manual.py
FONTE: Microdados PISA 2022 (OCDE) - Arquivo CY08MSP_STU_QQQ.sav

OBJETIVO:
Extração primária das médias de proficiência por Unidade da Federação (UF).
Utiliza a variável 'STRATUM' para identificar a localização geográfica dos alunos
e calcula a média dos 10 Plausible Values (PVs).
"""

import pandas as pd
import pyreadstat
import os
import sys

# --- MAPEAMENTO OFICIAL DE ESTRATOS (STRATUM -> UF) ---
# Padrão alfabético: AC, AL, AP, AM, BA... até TO.
MAPA_STRATUM_UF = {
    'BRA01': 'AC', 'BRA02': 'AL', 'BRA03': 'AP', 'BRA04': 'AM', 'BRA05': 'BA',
    'BRA06': 'CE', 'BRA07': 'DF', 'BRA08': 'ES', 'BRA09': 'GO', 'BRA10': 'MA',
    'BRA11': 'MT', 'BRA12': 'MS', 'BRA13': 'MG', 'BRA14': 'PA', 'BRA15': 'PB',
    'BRA16': 'PR', 'BRA17': 'PE', 'BRA18': 'PI', 'BRA19': 'RJ', 'BRA20': 'RN',
    'BRA21': 'RS', 'BRA22': 'RO', 'BRA23': 'RR', 'BRA24': 'SC', 'BRA25': 'SP',
    'BRA26': 'SE', 'BRA27': 'TO'
}

def extrair_pisa_por_uf():
    # 1. Definição de Caminhos
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(script_dir)
    
    input_file = os.path.join(base_dir, 'data', 'raw', 'pisa_2022', 'CY08MSP_STU_QQQ.sav')
    output_file = os.path.join(base_dir, 'data', 'processed', 'pisa_2022_calculado_oficial.csv')

    print(f"--- INICIANDO PROCESSAMENTO PISA 2022 ---")
    print(f"Lendo: {input_file}")

    if not os.path.exists(input_file):
        print(f"❌ ERRO: Arquivo não encontrado.")
        return

    # 2. Leitura Otimizada
    cols_pv = []
    for materia in ['MATH', 'READ', 'SCIE']:
        cols_pv.extend([f'PV{i}{materia}' for i in range(1, 11)])
    
    colunas_load = ['CNT', 'STRATUM'] + cols_pv

    try:
        print("⏳ Lendo arquivo SPSS (pode demorar alguns segundos)...")
        df, meta = pyreadstat.read_sav(input_file, usecols=colunas_load, disable_datetime_conversion=True)
    except Exception as e:
        print(f"❌ Erro na leitura: {e}")
        return

    # 3. Filtrar Brasil
    print("🇧🇷 Filtrando Brasil...")
    df_bra = df[df['CNT'] == 'BRA'].copy()
    
    if df_bra.empty:
        print("❌ ERRO: Brasil não encontrado.")
        return

    # 4. Decodificação Geográfica
    print("🗺️  Mapeando BRAxx -> Sigla UF...")
    df_bra['STRATUM'] = df_bra['STRATUM'].astype(str).str.strip()
    df_bra['SG_UF_PROVA'] = df_bra['STRATUM'].map(MAPA_STRATUM_UF)

    # 5. Cálculo das Médias
    print("🧮 Calculando médias (Plausible Values)...")
    df_bra['PISA_Matematica'] = df_bra[[f'PV{i}MATH' for i in range(1, 11)]].mean(axis=1)
    df_bra['PISA_Leitura']    = df_bra[[f'PV{i}READ' for i in range(1, 11)]].mean(axis=1)
    df_bra['PISA_Ciencias']   = df_bra[[f'PV{i}SCIE' for i in range(1, 11)]].mean(axis=1)

    # 6. Agrupar por UF
    print("📊 Consolidando tabela final...")
    df_final = df_bra.groupby('SG_UF_PROVA').agg({
        'PISA_Matematica': 'mean',
        'PISA_Leitura': 'mean',
        'PISA_Ciencias': 'mean',
        'CNT': 'count'
    }).reset_index()

    df_final.rename(columns={'CNT': 'Amostra_Alunos'}, inplace=True)
    
    # Média Geral e Arredondamento
    df_final['PISA_Geral'] = (df_final['PISA_Matematica'] + df_final['PISA_Leitura'] + df_final['PISA_Ciencias']) / 3
    df_final = df_final.round(2)

    # 7. Salvar e Mostrar TUDO
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df_final.to_csv(output_file, index=False, sep=';', encoding='utf-8-sig')

    print(f"✅ SUCESSO! Arquivo salvo: {output_file}")
    print(f"\n--- CONFERÊNCIA: {len(df_final)} UFs ENCONTRADAS ---")
    
    # Imprime todas as linhas para você mostrar ao professor
    print(df_final[['SG_UF_PROVA', 'PISA_Geral', 'Amostra_Alunos']].to_string(index=False))

    if len(df_final) == 27:
        print("\n✅ TODAS AS 27 UNIDADES DA FEDERAÇÃO FORAM PROCESSADAS.")
    else:
        print(f"\n⚠️ ATENÇÃO: Foram encontradas apenas {len(df_final)} UFs.")

if __name__ == "__main__":
    extrair_pisa_por_uf()