import pandas as pd
import os

def gerar_tabela_detalhada():
    # Caminhos
    arquivo_entrada = os.path.join('dados_brutos', 'Agregado-20251216034912.csv')
    arquivo_saida = os.path.join('dados', '3_saneamento_snis_detalhado.csv')

    print("🚀 GERANDO TABELA DETALHADA (LINHA A LINHA)...")

    try:
        # Lê o arquivo bruto
        df = pd.read_csv(
            arquivo_entrada, 
            sep=';', 
            encoding='utf-16le', 
            thousands='.', 
            decimal=',', 
            header=None,   # Ignoramos o cabeçalho original deslocado
            skiprows=1,    # Pulamos a primeira linha de títulos
            low_memory=False
        )
        
        # --- SELEÇÃO DAS COLUNAS IMPORTANTES ---
        # 1: Município (Nome)
        # 2: UF
        # 7: Abrangência (Local vs Regional - Importante para entender duplicatas)
        # 14: G12A (População Total IBGE)
        # 81: ES001 (População Atendida Esgoto)
        # 213: IN056 (Índice % calculado pelo governo)
        
        df_selecao = df.iloc[:, [1, 2, 7, 14, 81, 213]].copy()
        df_selecao.columns = ['Municipio', 'UF', 'Abrangencia', 'Pop_Total', 'Pop_Esgoto_Atendida', 'Indice_Esgoto_Pct']

        # Limpeza Numérica (Converter texto para float)
        def limpar_numero(x):
            if pd.isna(x) or str(x).strip() == '': return 0.0
            # Remove ponto de milhar e troca vírgula por ponto
            x = str(x).replace('.', '').replace(',', '.')
            return float(x)

        colunas_numericas = ['Pop_Total', 'Pop_Esgoto_Atendida', 'Indice_Esgoto_Pct']
        for col in colunas_numericas:
            df_selecao[col] = df_selecao[col].apply(limpar_numero)

        # Salva o arquivo detalhado
        # Usamos sep=';' para abrir fácil no Excel brasileiro se precisar
        df_selecao.to_csv(arquivo_saida, index=False, sep=';', encoding='utf-8-sig')
        
        print(f"\n✅ TABELA DETALHADA GERADA!")
        print(f"📂 Arquivo: {arquivo_saida}")
        print("\n--- Amostra (Primeiras 5 linhas) ---")
        print(df_selecao.head())
        
        print("\n--- Amostra (Casos Regionais) ---")
        print(df_selecao[df_selecao['Abrangencia'] == 'Regional'].head(3))

    except Exception as e:
        print(f"\n❌ Erro: {e}")

if __name__ == "__main__":
    gerar_tabela_detalhada()