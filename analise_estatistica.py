import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os

def analisar_correlacoes():
    print("🚀 Iniciando Análise de Correlações...")
    
    # Caminho do arquivo de entrada
    caminho_dataset = 'dados/dataset_mestre.csv'
    caminho_matriz = 'analise/matriz_correlacao.csv'
    caminho_heatmap = 'analise/heatmap_correlacao.png'
    
    # --- 1. Carregar Dados ---
    if not os.path.exists(caminho_dataset):
        print(f"❌ ARQUIVO NÃO ENCONTRADO: {caminho_dataset}")
        print("Execute o script 'cria_dataset_mestre.py' primeiro.")
        return
        
    df = pd.read_csv(caminho_dataset)
    print("✅ Dataset Mestre carregado.")
    
    # --- 2. Preparar Dados para Análise ---
    # Para a correlação, precisamos focar em um ano específico ou agregar os anos.
    # Vamos usar os dados do ano mais recente disponível no dataset.
    ano_recente = df['Ano'].max()
    df_analise = df[df['Ano'] == ano_recente]
    print(f"🔬 Analisando o ano de {ano_recente}.")
    
    # Selecionar apenas colunas numéricas relevantes
    colunas_numericas = df_analise.select_dtypes(include=['float64', 'int64']).columns
    df_numerico = df_analise[colunas_numericas]
    
    # Remover colunas que não são indicadores (como 'Ano')
    df_numerico = df_numerico.drop(columns=['Ano'], errors='ignore')
    
    # --- 3. Calcular Matriz de Correlação ---
    print("🧮 Calculando a Matriz de Correlação de Pearson...")
    matriz_corr = df_numerico.corr(method='pearson')
    
    # Garante a pasta de análise
    if not os.path.exists('analise'):
        os.makedirs('analise')
        
    # Salva a matriz em CSV
    matriz_corr.to_csv(caminho_matriz)
    print(f"💾 Matriz de correlação salva em: {caminho_matriz}")
    
    # --- 4. Gerar Heatmap ---
    print("🎨 Gerando o Heatmap...")
    
    plt.figure(figsize=(12, 10))
    sns.heatmap(
        matriz_corr, 
        annot=True, 
        cmap='coolwarm', 
        fmt=".2f",
        linewidths=.5
    )
    plt.title(f'Matriz de Correlação (Ano: {ano_recente})', fontsize=16)
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)
    plt.tight_layout()
    
    # Salva o gráfico
    plt.savefig(caminho_heatmap)
    print(f"🖼️ Heatmap salvo em: {caminho_heatmap}")
    
    print("\n🏆 SUCESSO! Análise concluída.")

if __name__ == "__main__":
    analisar_correlacoes()
