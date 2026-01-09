import zipfile
import pandas as pd
import os

# Caminho para o seu arquivo (ajuste conforme necessário)
zip_path = 'data/raw/microdados_saeb_2015.zip' 

print(f"--- INSPECIONANDO: {os.path.basename(zip_path)} ---")

try:
    with zipfile.ZipFile(zip_path, 'r') as z:
        # Tenta achar o arquivo de escolas
        target_file = next((f for f in z.namelist() if 'TS_ESCOLA' in f and f.endswith('.csv')), None)
        
        if target_file:
            print(f"Arquivo encontrado: {target_file}")
            with z.open(target_file) as f:
                # Lê apenas a primeira linha (cabeçalho)
                header = pd.read_csv(f, sep=';', encoding='latin1', nrows=0).columns.tolist()
                
                print("\nLISTA DE COLUNAS DISPONÍVEIS:")
                print(header)
                
                # Verificação específica
                has_3em = any('3EM' in col for col in header)
                has_serie = 'ID_SERIE' in header
                
                print("\n--- DIAGNÓSTICO ---")
                if has_3em:
                    print("✅ FORMATO LARGO (WIDE): Colunas específicas '3EM' detectadas.")
                    print("   -> Seu script original deve funcionar corretamente.")
                elif has_serie:
                    print("⚠️ FORMATO LONGO (LONG): Coluna 'ID_SERIE' detectada.")
                    print("   -> SEU SCRIPT ORIGINAL ESTÁ ERRADO!")
                    print("   -> Ele vai misturar as séries. Você precisa filtrar onde ID_SERIE == 12 (Ensino Médio).")
                else:
                    print("❓ FORMATO DESCONHECIDO: Verifique os nomes das colunas acima.")
        else:
            print("❌ Arquivo TS_ESCOLA não encontrado no ZIP.")
except Exception as e:
    print(f"Erro: {e}")