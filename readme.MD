# Capital cognitivo e supressão ambiental no Brasil (2022-2024)

> **Status:** Em desenvolvimento (Semana 1/4)
> **Stack:** Python, Pandas, Sidrapy, Estatística Espacial

## Visão geral do projeto

Este repositório contém o código-fonte e a documentação técnica para o projeto de pesquisa **"Capital cognitivo e supressão ambiental: uma réplica estendida e modelagem econométrica das 27 unidades federativas do Brasil"**.

O estudo propõe uma réplica estendida do trabalho de Lynn, Antonelli-Ponti, Silva & Meisenberg (2017), investigando a estabilidade temporal das correlações entre **Inteligência Nacional (QI/Capital Cognitivo)** e indicadores socioeconômicos. A inovação deste projeto reside na incorporação de dados pós-pandemia (2022-2024) e na testagem da **Hipótese da Supressão Ambiental**, utilizando modelagem espacial para verificar se fatores como saneamento precário e violência atuam como freios biológicos ao desenvolvimento cognitivo.

## Objetivos técnicos (Engenharia de Dados)

Como parte de um portfólio de Engenharia de Dados, este projeto substitui a coleta manual por um pipeline de dados automatizado (ETL) que:

1.  **Extrai (Extract):** Consome dados diretamente de APIs públicas (IBGE/SIDRA, Localidades) e processa microdados brutos (INEP/ENEM).
2.  **Transforma (Transform):** Realiza limpeza, normalização (z-scores) e agregação por Unidade Federativa.
3.  **Carrega (Load):** Consolida um "Dataset Mestre" ecológico pronto para análise econométrica.

## Estrutura de dados

O nível de agregação é a Unidade Federativa (UF). As principais dimensões analisadas são:

* **Cognitiva:** Média ENEM (2022-2024), PISA 2022.
* **Econômica:** PIB per capita, Índice de Gini.
* **Infraestrutura:** Água/Esgoto (SNIS), Acesso à Internet Banda Larga.
* **Social/Demográfica:** Taxa de Fecundidade, IDH.
* **Segurança:** Mortes Violentas Intencionais (MVI).

## Estrutura do repositório

```text
.
├── data/                  # Armazenamento de dados (brutos e processados)
├── src/                   # Código fonte dos scripts de ETL
│   ├── etl_semana_1.py    # Coleta de dados básicos (IBGE/População/PIB)
│   └── ...
├── notebooks/             # Jupyter Notebooks para análise exploratória e regressão
├── requirements.txt       # Dependências do projeto
└── README.md              # Documentação
```

## Instalação e execução

### Pré-requisitos

* Python 3.8 ou superior
* Pip

### Passo a passo

1.  **Clone o repositório:**
    ```bash
    git clone [https://github.com/seu-usuario/analise-capital-cognitivo-brasil.git](https://github.com/seu-usuario/analise-capital-cognitivo-brasil.git)
    cd analise-capital-cognitivo-brasil
    ```

2.  **Crie e ative um ambiente virtual (recomendado):**
    ```bash
    python -m venv venv
    # No Windows:
    venv\Scripts\activate
    # No Linux/Mac:
    source venv/bin/activate
    ```

3.  **Instale as dependências:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Execute o pipeline da Semana 1:**
    Este script gera a base das UFs e coleta dados do Censo 2022 e PIB via API do IBGE.
    ```bash
    python src/etl_semana_1.py
    ```

## Cronograma de desenvolvimento

* [x] **Semana 1:** Arquitetura do projeto, coleta de dados demográficos e econômicos (API IBGE).
* [ ] **Semana 2:** Processamento de dados complexos (Saneamento/SNIS e Microdados ENEM).
* [ ] **Semana 3:** Consolidação do Dataset Mestre e validação estatística.
* [ ] **Semana 4:** Análise espacial (Índice de Moran), Regressão Linear Múltipla e Relatório Final.

## Autores e pesquisadores

* **Dr. José Aparecido da Silva** (Pesquisador Sênior)
* **Me. Cássio Dalbem Barth** (Engenheiro de Dados e Pesquisador)

## Referências principais

* Lynn, R., Antonelli-Ponti, M., Mazzei, R. F., Silva, J. A., & Meisenberg, G. (2017). *Differences in Intelligence and Socio-Economic Outcomes across the Twenty Seven States of Brazil*. Mankind Quarterly.
* Rindermann, H. (2018). *Cognitive Capitalism*. Cambridge University Press.

---
*Este projeto é estritamente acadêmico e utiliza dados públicos oficiais para fins de análise estatística agregada.*