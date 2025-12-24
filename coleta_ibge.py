import sidrapy
import pandas as pd
import os
import time

def get_pib_historico():
    print("⏳ Consultando PIB (Série 2010-2022+)...")
    # Tabela 5938: PIB a preços correntes
    # Variável 37: PIB per capita
    # Periodo: "all" (Traz todos os anos disponíveis na tabela)
    try:
        data = sidrapy.get_table(
            table_code="5938", territorial_level="3", ibge_territorial_code="all",
            variable="37", period="all", header="n"
        )
        # Colunas: D1N (UF), D2N (Ano), V (Valor)
        df = data.iloc[1:][['D1N', 'D2N', 'V']].copy()
        df.columns = ['UF', 'Ano', 'PIB_per_capita']
        return df
    except Exception as e:
        print(f"Erro no PIB: {e}")
        return pd.DataFrame()

def get_populacao_estimada():
    print("⏳ Consultando Estimativas de População (Série Anual)...")
    # Tabela 6579: Estimativas de População
    # Variável 9324: População residente estimada
    # Periodo: "all"
    try:
        data = sidrapy.get_table(
            table_code="6579", territorial_level="3", ibge_territorial_code="all",
            variable="9324", period="all", header="n"
        )
        df = data.iloc[1:][['D1N', 'D2N', 'V']].copy()
        df.columns = ['UF', 'Ano', 'Populacao_Estimada']
        return df
    except Exception as e:
        print(f"Erro na População: {e}")
        return pd.DataFrame()

def processar_dados():
    # 1. Coleta
    df_pib = get_pib_historico()
    df_pop = get_populacao_estimada()
    
    # 2. Tratamento de Tipos
    # Converter valores numéricos
    df_pib['PIB_per_capita'] = pd.to_numeric(df_pib['PIB_per_capita'], errors='coerce')
    df_pop['Populacao_Estimada'] = pd.to_numeric(df_pop['Populacao_Estimada'], errors='coerce')
    
    # Padronizar Ano (às vezes vem como texto "2010")
    df_pib['Ano'] = df_pib['Ano'].astype(int)
    df_pop['Ano'] = df_pop['Ano'].astype(int)

    # 3. Merge (Juntar as tabelas por UF e ANO)
    print("🔗 Cruzando dados por UF e Ano...")
    df_final = pd.merge(df_pib, df_pop, on=['UF', 'Ano'], how='inner')
    
    # 4. Ordenação
    df_final = df_final.sort_values(['UF', 'Ano']).reset_index(drop=True)
    
    return df_final

if __name__ == "__main__":
    try:
        # Garante a pasta
        if not os.path.exists('dados'):
            os.makedirs('dados')

        df_completo = processar_dados()
        
        if not df_completo.empty:
            print(f"\n✅ SUCESSO! Coletados {len(df_completo)} registros.")
            print("Amostra (Primeiras linhas):")
            print(df_completo.head())
            print("\nAmostra (Últimas linhas):")
            print(df_completo.tail())
            
            caminho = 'dados/1_ibge_historico_painel.csv'
            df_completo.to_csv(caminho, index=False)
            print(f"\n💾 Arquivo salvo em: {caminho}")
        else:
            print("❌ Nenhum dado foi coletado. Verifique sua conexão.")
            
    except Exception as e:
        print(f"❌ Erro Crítico: {e}")