import os
import pandas as pd
import unicodedata


def normalize_text(s):
    if pd.isna(s):
        return s
    s = str(s)
    s = s.strip()
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not unicodedata.combining(c))
    return s


NAME_TO_UF = {
    'acre': 'AC', 'alagoas': 'AL', 'amapa': 'AP', 'amazonas': 'AM', 'bahia': 'BA',
    'ceara': 'CE', 'distrito federal': 'DF', 'espirito santo': 'ES', 'goias': 'GO',
    'maranhao': 'MA', 'mato grosso': 'MT', 'mato grosso do sul': 'MS', 'minas gerais': 'MG',
    'para': 'PA', 'paraiba': 'PB', 'parana': 'PR', 'pernambuco': 'PE', 'piaui': 'PI',
    'rio de janeiro': 'RJ', 'rio grande do norte': 'RN', 'rio grande do sul': 'RS',
    'rondonia': 'RO', 'roraima': 'RR', 'santa catarina': 'SC', 'sao paulo': 'SP',
    'sergipe': 'SE', 'tocantins': 'TO'
}


def main():
    root = os.path.dirname(__file__)
    dados_dir = os.path.join(root, 'dados')

    path_ibge = os.path.join(dados_dir, '1_ibge_historico_painel.csv')
    path_enem = os.path.join(dados_dir, '2_enem_historico_2022_2024.csv')
    path_snis = os.path.join(dados_dir, '3_saneamento_snis_detalhado.csv')
    path_homic = os.path.join(dados_dir, '4_seguranca_homicidios.csv')

    # Read IBGE (state names)
    df_ibge = pd.read_csv(path_ibge)
    df_ibge['UF_norm'] = df_ibge['UF'].apply(lambda x: normalize_text(x).lower())
    df_ibge['UF_code'] = df_ibge['UF_norm'].map(NAME_TO_UF)
    # Keep most recent year per state
    df_ibge_latest = df_ibge.loc[df_ibge.groupby('UF_code')['Ano'].idxmax()].copy()
    df_ibge_latest = df_ibge_latest[['UF_code', 'Ano', 'PIB_per_capita', 'Populacao_Estimada']]
    df_ibge_latest = df_ibge_latest.rename(columns={'UF_code': 'UF', 'Ano': 'Ano_IBGE'})

    # Read ENEM (UF codes)
    df_enem = pd.read_csv(path_enem)
    df_enem_latest = df_enem.loc[df_enem.groupby('UF')['Ano'].idxmax()].copy()
    df_enem_latest = df_enem_latest[['UF', 'MEDIA_GERAL', 'Ano']].rename(columns={'MEDIA_GERAL': 'ENEM_media', 'Ano': 'Ano_ENEM'})

    # Read SNIS (semicolon)
    df_snis = pd.read_csv(path_snis, sep=';')
    # Ensure numeric
    df_snis['Pop_Total'] = pd.to_numeric(df_snis['Pop_Total'], errors='coerce')
    df_snis['Pop_Esgoto_Atendida'] = pd.to_numeric(df_snis['Pop_Esgoto_Atendida'], errors='coerce')
    df_snis_uf = df_snis.groupby('UF')[['Pop_Total', 'Pop_Esgoto_Atendida']].sum().reset_index()
    df_snis_uf['Saneamento_Cobertura_Esgoto'] = (df_snis_uf['Pop_Esgoto_Atendida'] / df_snis_uf['Pop_Total'] * 100).round(2)

    # Read homicide rates
    df_hom = pd.read_csv(path_homic)

    # Build master list of UFs (take union)
    ufs = sorted(list(set(df_ibge_latest['UF'].dropna().tolist() + df_enem_latest['UF'].dropna().tolist() + df_snis_uf['UF'].dropna().tolist() + df_hom['UF'].dropna().tolist())))
    master = pd.DataFrame({'UF': ufs})

    # Merge
    master = master.merge(df_ibge_latest[['UF', 'PIB_per_capita', 'Populacao_Estimada', 'Ano_IBGE']], on='UF', how='left')
    master = master.merge(df_enem_latest[['UF', 'ENEM_media', 'Ano_ENEM']], on='UF', how='left')
    master = master.merge(df_snis_uf[['UF', 'Pop_Total', 'Pop_Esgoto_Atendida', 'Saneamento_Cobertura_Esgoto']], on='UF', how='left')
    master = master.merge(df_hom[['UF', 'Taxa_Homicidios']], on='UF', how='left')

    # Reorder columns
    cols = ['UF', 'ENEM_media', 'Ano_ENEM', 'PIB_per_capita', 'Populacao_Estimada', 'Pop_Total', 'Pop_Esgoto_Atendida', 'Saneamento_Cobertura_Esgoto', 'Taxa_Homicidios', 'Ano_IBGE']
    master = master[cols]

    out_path = os.path.join(dados_dir, 'master_dataset_27uf.csv')
    master.to_csv(out_path, index=False)

    print('\nMaster dataset criado em:', out_path)
    print(master.to_string(index=False))


if __name__ == '__main__':
    main()
