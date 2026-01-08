import pandas as pd
import os
import zipfile
import numpy as np

# Configurações de caminhos
BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_RAW = os.path.join(BASE_PATH, 'data', 'raw')
DATA_PROCESSED = os.path.join(BASE_PATH, 'data', 'processed')
os.makedirs(DATA_PROCESSED, exist_ok=True)

# Mapeamento flexível de colunas (Ano -> {alvo: [possiveis_nomes]})
COLUMN_VARIANTS = {
    'uf': ['ID_UF', 'ID_UF_RESIDENCIA', 'CO_UF'],
    'lp': ['PROFICIENCIA_LP', 'PROFICIENCIA_LP_SAEB', 'MEDIA_LP'],
    'mt': ['PROFICIENCIA_MT', 'PROFICIENCIA_MT_SAEB', 'MEDIA_MT']
}

def resolve_column(df, target_key):
    """Retorna a primeira coluna encontrada no DF que corresponde às variantes."""
    possibilities = COLUMN_VARIANTS.get(target_key, [])
    for p in possibilities:
        if p in df.columns:
            return p
    return None

def load_saeb_year(year, zip_filename):
    zip_path = os.path.join(DATA_RAW, zip_filename)
    if not os.path.exists(zip_path):
        print(f"[WARNING] Arquivo não encontrado: {zip_filename}")
        return None

    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            # Busca arquivo TS_ESCOLA ou TS_ALUNO
            files = z.namelist()
            target = next((f for f in files if 'TS_ESCOLA' in f and f.endswith('.csv')), None)
            
            if not target:
                print(f"[ERROR] TS_ESCOLA não encontrado em {zip_filename}")
                return None
            
            print(f"[INFO] Processando {target}...")
            
            # Lê apenas as primeiras linhas para pegar o header correto
            # O separador pode variar entre ';' e ','
            try:
                df_iter = pd.read_csv(z.open(target), sep=';', encoding='latin1', iterator=True, chunksize=10000)
                df_chunk = next(df_iter)
            except:
                 df_iter = pd.read_csv(z.open(target), sep=',', encoding='latin1', iterator=True, chunksize=10000)
                 df_chunk = next(df_iter)

            # Resolve nomes das colunas
            col_uf = resolve_column(df_chunk, 'uf')
            col_lp = resolve_column(df_chunk, 'lp')
            col_mt = resolve_column(df_chunk, 'mt')
            
            if not all([col_uf, col_lp, col_mt]):
                print(f"[ERROR] Colunas críticas não encontradas no ano {year}. Header: {list(df_chunk.columns)}")
                return None
            
            # Recarrega dataset inteiro apenas com colunas úteis
            use_cols = [col_uf, col_lp, col_mt]
            df = pd.read_csv(z.open(target), sep=';', encoding='latin1', usecols=use_cols)
            
            # Renomeia para padrão
            df = df.rename(columns={col_uf: 'ID_UF', col_lp: 'PROFICIENCIA_LP', col_mt: 'PROFICIENCIA_MT'})
            df['NU_ANO'] = year
            
            # Tratamento numérico (vírgula para ponto)
            for col in ['PROFICIENCIA_LP', 'PROFICIENCIA_MT']:
                if df[col].dtype == object:
                    df[col] = df[col].astype(str).str.replace(',', '.').astype(float)
            
            return df

    except Exception as e:
        print(f"[ERROR] Falha no processamento SAEB {year}: {e}")
        return None

def main():
    dfs = []
    for year, file in [(2015, 'microdados_saeb_2015.zip'), (2017, 'microdados_saeb_2017.zip')]:
        df = load_saeb_year(year, file)
        if df is not None:
            dfs.append(df)
    
    if dfs:
        full = pd.concat(dfs)
        # Agregação por UF
        grouped = full.groupby(['NU_ANO', 'ID_UF'])[['PROFICIENCIA_LP', 'PROFICIENCIA_MT']].mean().reset_index()
        
        out_path = os.path.join(DATA_PROCESSED, 'saeb_historical_consolidated.csv')
        grouped.to_csv(out_path, index=False)
        print(f"[SUCCESS] Arquivo consolidado salvo: {out_path}")

if __name__ == "__main__":
    main()