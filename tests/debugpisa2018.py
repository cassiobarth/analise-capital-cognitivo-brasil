import pandas as pd
import os

# Caminho do arquivo
BASE_DIR = os.getcwd()
FILE_PATH = os.path.join(BASE_DIR, 'data', 'raw', 'Pisa', 'pisa_2018', 'CY07_MSU_STU_QQQ.sav')

def investigate():
    print("--- INVESTIGAÇÃO PROFUNDA PISA 2018 ---")
    
    if not os.path.exists(FILE_PATH):
        print("Arquivo não encontrado.")
        return

    # Ler APENAS o cabeçalho (meta) para ser rápido
    # O pd.read_spss carrega tudo, então vamos ler poucas linhas se possível, 
    # mas para ver colunas precisamos ler o meta.
    print("[INFO] Lendo metadados do arquivo...")
    
    # Lendo apenas 10 linhas para pegar as colunas
    try:
        df_iter = pd.read_spss(FILE_PATH, convert_categoricals=False)
        # Cortamos logo para não pesar a memória, só queremos ver as colunas
        df = df_iter.head(100)
    except Exception as e:
        print(f"Erro: {e}")
        return

    print(f"\n[INFO] Total de Colunas no arquivo: {len(df.columns)}")
    
    # 1. Procurar por colunas suspeitas de conter região/estado
    keywords = ['REG', 'SUB', 'STRAT', 'BRA', 'LOC', 'STATE', 'UF', 'PROV']
    suspect_cols = [c for c in df.columns if any(k in c.upper() for k in keywords)]
    
    print("\n[ALVO] Colunas suspeitas encontradas:")
    print(suspect_cols)
    
    # 2. Se SUBNATIO existir, vamos ver o que tem nela filtrando Brasil
    if 'SUBNATIO' in df.columns:
        print("\n[INVESTIGAÇÃO] Verificando conteúdo de SUBNATIO para o Brasil...")
        # Recarregar filtrando Brasil se necessário, ou checar no que já temos
        # Como o read_spss não suporta 'chunksize' nativo facilmente como csv, 
        # vamos confiar que a leitura completa anterior funcionou ou ler de novo focado.
        
        df_bra = df_iter[df_iter['CNT'] == 'BRA']
        print(f"Valores únicos em SUBNATIO (Brasil):")
        print(df_bra['SUBNATIO'].unique())
    else:
        print("\n[FALHA] A coluna SUBNATIO não existe neste arquivo.")

if __name__ == "__main__":
    investigate()