import pandas as pd
import requests
import sidrapy
import time
from datetime import datetime

class ColetorDadosBrasil:
    def __init__(self):
        self.df_mestre = pd.DataFrame()
        print(f"[{datetime.now()}] Iniciando coletor de dados ecológicos...")

    def gerar_base_ufs(self):
        """
        Cria a espinha dorsal do dataset buscando as 27 UFs
        na API de Localidades do IBGE.
        """
        print("--- Gerando base de UFs ---")
        url = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
        response = requests.get(url)
        
        if response.status_code == 200:
            dados_ufs = response.json()
            # Criando DataFrame inicial
            df = pd.DataFrame(dados_ufs)
            df = df[['id', 'sigla', 'nome']].rename(columns={'id': 'codigo_ibge'})
            
            # Ordenar por código para manter padrão (Norte -> Sul) ou alfabético
            self.df_mestre = df.sort_values('codigo_ibge').reset_index(drop=True)
            print(f"Base de UFs gerada com sucesso: {len(self.df_mestre)} unidades.")
        else:
            raise Exception("Erro ao conectar com API de Localidades do IBGE")

    def buscar_populacao_censo(self):
        """
        Busca a população residente (Censo 2022) via API SIDRA.
        Tabela 4714 (População Residente).
        """
        print("--- Coletando População (Censo 2022) ---")
        
        # Parâmetros para o sidrapy (Tabela 4714)
        dados = sidrapy.get_table(
            table_code="4714",
            territorial_level="3",  # 3 = Unidade da Federação
            ibge_territorial_code="all",
            variable="93",          # População residente
            period="2022"           # Ano do Censo
        )
        
        # Limpeza do retorno do sidrapy
        if not dados.empty:
            # A primeira linha costuma ser cabeçalho/metadados no sidrapy
            dados.columns = dados.iloc[0]
            dados = dados.iloc[1:]
            
            # Selecionar e renomear
            dados = dados[['Unidade da Federação (Código)', 'Valor']]
            dados.columns = ['codigo_ibge', 'populacao_2022']
            
            # Converter tipos
            dados['codigo_ibge'] = dados['codigo_ibge'].astype(int)
            dados['populacao_2022'] = dados['populacao_2022'].astype(int)
            
            # Merge com a base mestre
            self.df_mestre = pd.merge(self.df_mestre, dados, on='codigo_ibge', how='left')
            print("Dados de população integrados.")
        else:
            print("ERRO: Falha ao coletar População.")

    def buscar_pib_per_capita(self):
        """
        Busca o PIB per capita (Contas Regionais) via API SIDRA.
        Tabela 5938 (PIB - Série revisada).
        Nota: O dado mais recente consolidado costuma ter defasagem de 2 anos.
        Vamos tentar pegar o último disponível (geralmente 2021 ou 2022).
        """
        print("--- Coletando PIB per capita ---")
        
        # Tabela 5938, Variável 37 (PIB per capita a preços correntes)
        dados = sidrapy.get_table(
            table_code="5938",
            territorial_level="3",
            ibge_territorial_code="all",
            variable="37",
            period="last" # Pega o último disponível
        )
        
        if not dados.empty:
            dados.columns = dados.iloc[0]
            dados = dados.iloc[1:]
            
            dados = dados[['Unidade da Federação (Código)', 'Valor']]
            dados.columns = ['codigo_ibge', 'pib_per_capita']
            
            dados['codigo_ibge'] = dados['codigo_ibge'].astype(int)
            # PIB vem como string com ponto ou vírgula, tratar float
            dados['pib_per_capita'] = dados['pib_per_capita'].astype(float)
            
            self.df_mestre = pd.merge(self.df_mestre, dados, on='codigo_ibge', how='left')
            print("Dados de PIB integrados.")
        else:
            print("ERRO: Falha ao coletar PIB.")

    def placeholder_internet(self):
        """
        Prepara a coluna de Internet (Acesso Banda Larga).
        Como a PNAD TIC exige parâmetros complexos, vamos criar a coluna vazia
        para preenchimento posterior ou busca específica na Semana 2.
        """
        print("--- Preparando estrutura para Internet/Infra ---")
        self.df_mestre['acesso_internet_pct'] = None # Placeholder
        self.df_mestre['esgoto_sanitario_pct'] = None # Placeholder

    def salvar_dataset_parcial(self):
        filename = f"dataset_semana_1_{datetime.now().strftime('%Y%m%d')}.csv"
        self.df_mestre.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"--- Arquivo salvo: {filename} ---")
        print(self.df_mestre.head())

# Execução do Pipeline
if __name__ == "__main__":
    job = ColetorDadosBrasil()
    job.gerar_base_ufs()
    job.buscar_populacao_censo()
    job.buscar_pib_per_capita()
    job.placeholder_internet()
    job.salvar_dataset_parcial()