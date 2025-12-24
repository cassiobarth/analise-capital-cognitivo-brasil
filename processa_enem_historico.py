import pandas as pd
import os

def processar_snis_final():
    arquivo_entrada = os.path.join('dados_brutos', 'Agregado-20251216034912.csv')
    arquivo_saida = os.path.join('dados', '3_saneamento_snis_2022.csv')

    print("🚀 PROCESSANDO SANEAMENTO (ESTRATÉGIA PONDERADA)...")

    try:
        # Lê o arquivo ignorando o cabeçalho deslocado (header=None)
        # Usa 'utf-16le' que funcionou para você
        df = pd.read_csv(
            arquivo_entrada, 
            sep=';', 
            encoding='utf-16le', 
            thousands='.', 
            decimal=',', 
            header=None, 
            skiprows=1,
            low_memory=False
        )
        
        # Mapeamento fixo das colunas que você escolheu:
        # 2: UF, 14: G12A (Pop), 81: ES001 (Esgoto Abs), 213: IN056 (Índice %)
        df = df.iloc[:, [2, 14, 81, 213]].copy()
        df.columns = ['UF', 'Pop_Total', 'Pop_Esgoto_Abs', 'Indice_Esgoto_Pct']

        # Limpeza Numérica
        def limpar(x):
            if pd.isna(x) or str(x).strip() == '': return 0.0
            x = str(x).replace('.', '').replace(',', '.')
            return float(x)

        for col in ['Pop_Total', 'Pop_Esgoto_Abs', 'Indice_Esgoto_Pct']:
            df[col] = df[col].apply(limpar)

        print("   -> Calculando População Atendida (Recuperação de dados falhos)...")
        # TRUQUE: Se o número absoluto (ES001) for 0 ou NaN, mas tivermos o Índice (IN056),
        # reconstruímos a população atendida: (Indice / 100) * Pop_Total
        # Isso salva casos como Rio Branco!
        
        df['Pop_Atendida_Calculada'] = df['Pop_Esgoto_Abs']
        
        # Onde o absoluto for 0 e o índice > 0, usamos o índice
        mask_recover = (df['Pop_Esgoto_Abs'] == 0) & (df['Indice_Esgoto_Pct'] > 0)
        df.loc[mask_recover, 'Pop_Atendida_Calculada'] = (df.loc[mask_recover, 'Indice_Esgoto_Pct'] / 100) * df.loc[mask_recover, 'Pop_Total']

        # Agrupamento por Estado
        print("   -> Consolidando por UF...")
        df_estado = df.groupby('UF')[['Pop_Total', 'Pop_Atendida_Calculada']].sum().reset_index()

        # Cálculo Final da Porcentagem Estadual
        df_estado['Saneamento_Esgoto_Pct'] = (df_estado['Pop_Atendida_Calculada'] / df_estado['Pop_Total']) * 100
        
        # Arredonda e ordena
        df_final = df_estado[['UF', 'Saneamento_Esgoto_Pct']].round(2).sort_values('Saneamento_Esgoto_Pct', ascending=False)

        # Salva
        df_final.to_csv(arquivo_saida, index=False)
        print(f"\n✅ SUCESSO! Base de Saneamento gerada com precisão.")
        print(f"💾 Salvo em: {arquivo_saida}")
        print("\nTOP 5 ESTADOS (Melhor Saneamento):")
        print(df_final.head())
        print("\nTOP 5 ESTADOS (Pior Saneamento):")
        print(df_final.tail())

    except Exception as e:
        print(f"\n❌ Erro crítico: {e}")

if __name__ == "__main__":
    processar_snis_final()