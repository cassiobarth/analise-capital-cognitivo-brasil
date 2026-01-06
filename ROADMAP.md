# 🗺️ Roteiro Metodológico v2: A Rigidez Estrutural do Capital Cognitivo

## 1. Variáveis Independentes (O "X" da Questão)
*Nesta nova abordagem, dividimos o Capital Cognitivo em "Estoque Histórico" (Causa) e "Desempenho Atual" (Medição).*

### A. O Estoque de Capital Cognitivo (Preditores / Baseline)
*Dados utilizados para prever o desempenho futuro e provar a inércia estrutural.*
- **PISA 2015 & 2018 (Microdados Estaduais):**
  - **Função:** Variável independente defasada ($t-1$). Serve como a "âncora" de qualidade internacional.
  - **Nome do script:** `src/04_extrair_dados_pisa_historico.py`
  - **Fonte:** OCDE / INEP (Base com *oversampling* estadual).

### B. Os Indicadores de Desempenho Atual (Proxies / Targets)
*Dados utilizados como a medida contemporânea de capital humano ($t$).*
- **ENEM (2022, 2023, 2024):**
  - **Função:** Mede o desempenho da "elite escolar" (concluintes aptos ao ensino superior).
  - **Nome do script:** `src/01_extrair_microdados_enem.py`
  - **Fonte:** INEP (Microdados Censitários dos Inscritos).
  
- **SAEB 2023 (Ensino Médio):**
  - **Função:** Mede o desempenho da "massa" (censitário da rede pública/privada), usado para triangulação.
  - **Nome do script:** `src/05_extrair_saeb_2023.py`
  - **Fonte:** INEP (Avaliação Nacional da Educação Básica).

---

## 2. Variáveis Dependentes (O "Y" - Impacto Socioeconômico)
*Indicadores que serão explicados pela regressão com o Capital Cognitivo.*

### A. Infraestrutura e Saneamento
- **Esgotamento sanitário e água encanada:** `src/indicadores/04_extrair_dados_snis.py` (SNIS).
- **Internet banda larga:** `src/indicadores/04_extrair_internet_pnad.py` (IBGE/PNAD).

### B. Riqueza e Desigualdade
- **PIB per capita estadual:** `src/indicadores/04_extrair_pib_capita.py` (IBGE/Contas Regionais).
- **Renda domiciliar e Gini:** `src/indicadores/04_extrair_rendimento_ibge.py` (IBGE/PNAD).

### C. Capital Humano Contextual
- **Escolaridade dos pais:** `src/indicadores/04_extrair_educacao_ibge.py` (IBGE).
- **IDH Estadual:** `src/indicadores/04_extrair_idh_atlas.py` (Atlas Brasil).
- **Proficiência em Inglês (EF EPI):** `src/indicadores/04_extrair_ingles_ef.py` (EF Education First).

### D. Eficiência Escolástica
- **Distorção Idade-Série:** `src/indicadores/04_extrair_fluxo_inep.py` (INEP).
- **Investimento por Aluno:** `src/indicadores/04_extrair_investimento_siope.py` (FNDE).

---

## 3. Observações Metodológicas Cruciais (Atualizado)

1.  **A Tese da Rigidez Estrutural:**
    A análise não se baseia apenas na correlação contemporânea, mas na **validade preditiva**.
    * *Evidência:* O PISA dois mil e dezoito prediz o ENEM dois mil e vinte e quatro com uma correlação de zero vírgula nove seis (r = 0,96).
    * Isso justifica o uso dos dados históricos como proxy estrutural para os estados, contornando a falha amostral do PISA dois mil e vinte e dois.

2.  **Triangulação de Dados (Elite vs. Massa):**
    Utiliza-se o **ENEM** (seleção voluntária) e o **SAEB** (censo obrigatório) simultaneamente para provar que o ranking de Capital Cognitivo é consistente, independentemente se avaliamos o topo ou a base da pirâmide educacional.

3.  **Auditoria do PISA 2022:**
    Os dados estaduais do PISA dois mil e vinte e dois foram auditados e **descartados** para fins de regressão estadual devido à ausência de estratificação por UF no arquivo `.sav` original, sendo substituídos pela série SAEB/ENEM validada.

4.  **Chave Primária e Integridade:**
    Todos os *datasets* devem ser unidos pela chave `SG_UF` (Sigla da Unidade da Federação). Scripts de indicadores devem conter *health check* garantindo exatamente vinte e sete linhas (27 UFs) antes da exportação.