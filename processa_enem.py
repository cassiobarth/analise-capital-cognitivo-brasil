import pandas as pd
import zipfile
import os
import gc # Garbage Collector para limpar memória RAM

def processar_enem_2023():
    # Caminhos
    arquivo_zip = 'microdados_enem_2023.zip'
    caminho_entrada = os.path.join('dados_brutos', arquivo_zip)
    caminho_saida = os.path.join('dados', '2_enem_medias_2023.csv')

    print(f"🚀 Iniciando processamento: {arquivo_zip}")
    
    if not os.path.exists(caminho_entrada):
        print(f"❌ ARQUIVO NÃO ENCONTRADO: {caminho_entrada}")
        print("Certifique-se de ter baixado o ZIP no site do INEP.")
        return

    # Colunas essenciais (UF e Notas)
    # TP_PRESENCA... é importante para filtrar quem faltou (Nota zero não é nota real)
    cols = [
        'SG_UF_PROVA', 
        'NU_NOTA_CN', 'NU_NOTA_CH', 'NU_NOTA_LC', 'NU_NOTA_MT', 'NU_NOTA_REDACAO'
    ]
    
    # Acumuladores
    soma_notas = pd.DataFrame()
    contagem_alunos = pd.DataFrame()

    try:
        with zipfile.ZipFile(caminho_entrada) as z:
            # Pega o primeiro CSV que encontrar dentro do ZIP
            nome_csv = [f for f in z.namelist() if f.endswith('.csv')][0]
            print(f"📄 Lendo CSV interno: {nome_csv}")
            print("⏳ Processando em chunks (Isso vai levar uns minutos)...")

            # Lê o arquivo em pedaços de 100mil linhas
            chunks = pd.read_csv(
                z.open(nome_csv), 
                sep=';', 
                encoding='latin-1', 
                usecols=cols, 
                chunksize=100000
            )
            
            for i, chunk in enumerate(chunks):
                # Limpeza básica: Remove linhas onde todas as notas são NaN
                chunk = chunk.dropna(subset=['NU_NOTA_REDACAO']) 

                # Agrupa por Estado
                agrupado_soma = chunk.groupby('SG_UF_PROVA')[cols[1:]].sum()
                agrupado_count = chunk.groupby('SG_UF_PROVA')[cols[1:]].count()
                
                # Soma aos acumuladores totais
                soma_notas = agrupado_soma.add(soma_notas, fill_value=0)
                contagem_alunos = agrupado_count.add(contagem_alunos, fill_value=0)
                
                # Feedback visual a cada 1 milhão de linhas (10 chunks)
                if i % 10 == 0:
                    print(f"   -> Processados {(i+1)*100}k registros...", end='\r')
                
                # Libera memória
                del chunk
                gc.collect()

        print("\n⚡ Calculando médias finais...")
        # Média = Soma Total / Contagem Total
        df_medias = soma_notas / contagem_alunos
        
        # Cria Média Geral (Média simples das 5 áreas)
        df_medias['MEDIA_GERAL'] = df_medias.mean(axis=1)
        
        # Formatação
        df_medias = df_medias.reset_index().rename(columns={'SG_UF_PROVA': 'UF'})
        df_medias = df_medias.round(2)
        
        # Adiciona ano para compatibilidade futura
        df_medias['Ano'] = 2023
        
        # Salva
        df_medias.to_csv(caminho_saida, index=False)
        print(f"\n💾 SUCESSO! Arquivo salvo em: {caminho_saida}")
        print(df_medias.head())

    except Exception as e:
        print(f"\n❌ Erro Crítico: {e}")

if __name__ == "__main__":
    processar_enem_2023()