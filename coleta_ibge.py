import sidrapy
import pandas as pd
import os
import time

def get_pib_per_capita():
    print("⏳ Buscando PIB per Capita (Tabela 5938)...")
    # Tabela 5938: PIB a preços correntes
    # Variável 37: PIB per capita
    # Nível Territorial 3: Estados
    data = sidrapy.get_table(
        table_code="5938", territorial_level="3", ibge_territorial_code="all",
        variable="37", period="last", header="n"
    )
    # Limpeza
    df = data.iloc[1:][['D1N', 'V']].copy()
    df.columns = ['UF', 'PIB_per_capita']
    df['PIB_per_capita'] = pd.to_numeric(df['PIB_per_capita'])
    return df

def get_populacao_censo():
    print("⏳ Buscando População Censo 2022 (Tabela 4714)...")
    # Tabela 4714: População Residente (Censo 2022)
    # Variável 93: População residente
    data = sidrapy.get_table(
        table_code="4714", territorial_level="3", ibge_territorial_code="all",
        variable="93", period="last", header="n"
    )
    # Limpeza
    df = data.iloc[1:][['D1N', 'V']].copy()
    df.columns = ['UF', 'Populacao_2022']
    df['Populacao_2022'] = pd.to_numeric(df['Populacao_2022'])
    return df

def main():
    # 1. Coleta
    try:
        df_pib = get_pib_per_capita()
        df_pop = get_populacao_censo()
        
        # 2. Fusão (Join) dos dados pela coluna 'UF'
        print("🔗 Unificando tabelas...")
        df_final = pd.merge(df_pib, df_pop, on='UF', how='inner')
        
        # 3. Ordenação e Ajustes Finais
        df_final = df_final.sort_values('UF').reset_index(drop=True)
        
        # 4. Salvar
        if not os.path.exists('dados'):
            os.makedirs('dados')
            
        caminho_csv = 'dados/1_ibge_socioeconomico.csv'
        df_final.to_csv(caminho_csv, index=False)
        
        print(f"\n✅ SUCESSO! Dados do IBGE salvos em: {caminho_csv}")
        print("-" * 50)
        print(df_final.head()) # Mostra as primeiras linhas
        print("-" * 50)
        
    except Exception as e:
        print(f"❌ Erro na coleta: {e}")

if __name__ == "__main__":
    main()