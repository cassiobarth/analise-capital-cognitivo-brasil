# Capital Cognitivo e Supressão Ambiental: Uma Réplica Estendida (Brasil 2022-2024)

> *Investigação ecológica e modelagem econométrica das 27 unidades federativas do Brasil.*

---

## 📄 Sobre o Projeto

Este projeto é uma **réplica estendida e atualização** do estudo seminal de *Lynn, Antonelli-Ponti, Silva e Meisenberg (2017)*: *"Differences in Intelligence and Socio-Economic Outcomes across the Twenty Seven States of Brazil"*.

A relação entre inteligência psicométrica média e prosperidade das nações é um dos temas mais robustos da psicologia diferencial moderna (Rindermann, 2018). No entanto, o Brasil apresenta desigualdades ambientais extremas que podem atuar como "freios biológicos" ou **supressores ambientais**, impedindo a expressão plena do potencial genético (Capital Cognitivo).

### Problema de Investigação
*"Em que medida o Capital Cognitivo (medido via ENEM/PISA) mantém seu poder preditivo sobre o desenvolvimento econômico e social das UFs brasileiras quando controlado por variáveis de infraestrutura sanitária, violência e correlação espacial?"*

---

## 🎯 Objetivos Específicos

1.  **Réplica Científica:** Investigar a estabilidade temporal das correlações entre Inteligência e indicadores socioeconômicos encontradas em 2017, num cenário pós-pandemia (2022-2024).
2.  **Teste de Supressão Ambiental:** Avaliar se a falta de saneamento básico e a prevalência de doenças infecciosas atuam como variáveis mediadoras que anulam o impacto do QI na riqueza (PIB).
3.  **Modelagem Espacial:** Aplicar o Índice de Moran (Global e Local) para controlar a dependência espacial entre estados vizinhos (clusters Norte-Sul), superando limitações de análises puramente lineares.

---

## 📊 Metodologia e Variáveis

O estudo adota um desenho **ecológico transversal**, utilizando dados secundários oficiais coletados via scripts de ETL (Python).

| Dimensão | Variável | Fonte | Justificativa Teórica |
| :--- | :--- | :--- | :--- |
| **Cognitiva** | Média ENEM (2022-2024) | INEP | Proxy de inteligência geral (*g*) e capital humano. |
| **Cognitiva** | PISA 2022 | OCDE | Validação cruzada com padrão internacional. |
| **Econômica** | PIB per capita | IBGE | Variável dependente (desfecho econômico). |
| **Ambiental** | Cobertura Água/Esgoto | SNIS | Indicador de carga parasitária e saúde infantil. |
| **Ambiental** | Acesso à Internet | IBGE | Fator de estimulação cognitiva moderna. |
| **Social** | Taxa de Fecundidade | IBGE | Variável chave na Teoria de História de Vida (r/K). |
| **Social** | Taxa de Homicídios | FBSP | Indicador de estresse social e impulsividade. |

---

## 🚦 Status do Projeto

**Fase Atual:** 🚧 **Sprint 2: Ingestão de Dados Complexos**
*Foco: Processamento de microdados do INEP e tabelas municipais do SNIS.*

> 📅 **Planejamento:** Para ver o cronograma detalhado das 4 semanas de desenvolvimento, consulte o [ROADMAP.md](./ROADMAP.md).
>
> 📝 **Histórico Técnico:** Para acompanhar as decisões de engenharia de dados, consulte o [DEVLOG.md](./DEVLOG.md).

---

## 🛠️ Stack Tecnológico

O projeto segue a estrutura **Cookiecutter Data Science** para reprodutibilidade.

* **Linguagem:** Python 3.12+
* **Engenharia de Dados:** `pandas`, `requests`, `sidrapy` (API IBGE)
* **Análise Espacial:** `pysal`, `esda` (Índice de Moran), `splot`
* **Estatística/Econometria:** `statsmodels` (OLS, Regressão Stepwise), `scipy`
* **Visualização:** `seaborn`, `matplotlib`, `geopandas`

### Estrutura de Diretórios

    ├── data/
    │   ├── raw/       # Dados brutos imutáveis (Zips do ENEM, CSV do SNIS)
    │   └── processed/ # Base Mestra consolidada e limpa
    ├── logs/          # Logs de execução do pipeline
    ├── notebooks/     # Análises exploratórias e testes de hipótese (Jupyter)
    ├── src/           # Scripts de ETL e funções de suporte
    └── references/    # Artigos base em PDF e manuais

---

## 🚀 Como Reproduzir o Estudo

### 1. Clonar e Configurar Ambiente

    git clone [https://github.com/SEU_USUARIO/analise-capital-cognitivo-brasil.git](https://github.com/SEU_USUARIO/analise-capital-cognitivo-brasil.git)
    cd analise-capital-cognitivo-brasil

    # Criar ambiente virtual (Windows Git Bash)
    python -m venv venv
    source venv/Scripts/activate

    # Instalar dependências
    pip install -r requirements.txt

### 2. Executar Pipeline de ETL
O script principal conecta nas APIs do governo, baixa os indicadores demográficos/econômicos e gera a base preliminar.

    python src/etl_semana_1.py

### 3. Validar Integridade (Sanity Check)
Script de auditoria para garantir que não existem valores espúrios (ex: PIB per capita fora da ordem de grandeza).

    python src/validacao.py

---

## 📚 Referências Bibliográficas

* **Lynn, R., Antonelli-Ponti, M., Silva, J. A., & Meisenberg, G. (2017).** Differences in Intelligence and Socio-Economic Outcomes across the Twenty Seven States of Brazil. *Mankind Quarterly*, 58(2).
* **Rindermann, H. (2018).** *Cognitive Capitalism: Human Capital and the Wellbeing of Nations*. Cambridge University Press.
* **Becker, D., et al. (2024).** Unraveling the nexus: Culture, cognitive competence, and economic performance. *Intelligence*.

---
**Pesquisadores Responsáveis:**
Dr. José Aparecido da Silva
Me. Cássio Dalbem Barth