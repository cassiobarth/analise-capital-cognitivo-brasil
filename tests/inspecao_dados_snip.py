import pandas as pd
import os

# Define o caminho absoluto baseado na localização atual do usuário no terminal
base_dir = os.getcwd() 
raw_dir = os.path.join(base_dir, 'data', 'raw')

arquivos = [
    'ConsolidadoMunicipio-20251225031327.csv',
    'snis_municipios_2022.csv'
]

def inspecionar_arquivo(nome_arquivo):
    caminho = os.path.join(raw_dir, nome_arquivo)
    print(f"\n--- 📄 Inspecionando: {nome_arquivo} ---")
    
    if not os.path.exists(caminho):
        print(f"❌ Erro: O arquivo não foi encontrado em {caminho}")
        return

    # Lista de encodings para tentar, começando pelo mais provável (UTF-16LE)
    encodings_para_testar = ['utf-16-le', 'latin-1', 'utf-8']
    
    for enc in encodings_para_testar:
        try:
            # Lemos apenas as 5 primeiras linhas para verificar o cabeçalho
            df = pd.read_csv(caminho, nrows=5, sep=';', encoding=enc)
            
            # Se as colunas vierem com caracteres nulos, limpamos
            df.columns = [str(c).replace('\x00', '') for c in df.columns]
            
            print(f"✅ Sucesso com encoding: {enc}")
            print(f"Número total de colunas: {len(df.columns)}")
            print("Primeiras 20 colunas identificadas:")
            print(df.columns.tolist()[:20])
            
            # Busca por palavras-chave para ajudar no mapeamento
            alvos = ['UF', 'Estado', 'Município', 'IN055', 'IN056', 'Água', 'Esgoto']
            encontradas = [c for c in df.columns if any(a.lower() in str(c).lower() for a in alvos)]
            print(f"🔍 Colunas sugeridas para o projeto: {encontradas}")
            return # Sai do loop se conseguir ler com sucesso
            
        except Exception as e:
            print(f"⚠️ Tentativa com {enc} falhou.")

if __name__ == "__main__":
    for arquivo in arquivos:
        inspecionar_arquivo(arquivo)