import pandas as pd
import os

def get_seguranca():
    print("🚀 GERANDO DADOS DE VIOLÊNCIA (FONTE: ANUÁRIO 2023/FBSP)...")
    
    # Taxa de Mortes Violentas Intencionais por 100 mil habitantes (Ref. 2022)
    # Fonte Oficial: Fórum Brasileiro de Segurança Pública - Anuário 2023
    # Inserido manualmente para garantir estabilidade (links de governo mudam todo mês)
    dados = {
        'UF': [
            'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 
            'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 
            'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
        ],
        'Taxa_Homicidios': [
            28.6, 26.6, 50.6, 42.5, 47.1, 35.5, 11.3, 29.3, 22.0, 
            26.9, 29.3, 19.8, 12.6, 29.6, 24.3, 22.1, 31.8, 23.0, 
            27.9, 36.7, 19.8, 34.3, 30.2, 9.1, 8.4, 30.5, 27.7
        ]
    }
    
    df = pd.DataFrame(dados)
    
    # Ordenar do mais seguro para o mais violento
    df = df.sort_values('Taxa_Homicidios')
    
    # Salvar
    if not os.path.exists('dados'): os.makedirs('dados')
    caminho = os.path.join('dados', '4_seguranca_homicidios.csv')
    df.to_csv(caminho, index=False)
    
    print(f"✅ Arquivo de Segurança Salvo: {caminho}")
    print("\n--- Top 5 Estados Mais Seguros (Menor Taxa) ---")
    print(df.head())
    print("\n--- Top 5 Estados Mais Violentos (Maior Taxa) ---")
    print(df.tail())

if __name__ == "__main__":
    get_seguranca()