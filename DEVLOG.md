# Devlog: Análise do Capital Cognitivo Brasil

## [30/12/2025] Inversão do modelo causal: Capital cognitivo como preditor
Alinhamento conceitual com a perspectiva do Dr. José Aparecido da Silva. O capital cognitivo (ENEM/PISA) passa a ser tratado como variável independente (causa) do desenvolvimento.

### Novo Paradigma Metodológico
* Variáveis Independentes (IV): Resultados consolidados do ENEM e PISA.
* Variáveis Dependentes (DV): Indicadores socioeconômicos (PIB, IDH, Gini, Proficiência em Inglês).
* Objetivo da Análise: Demonstrar como o abismo de sessenta e quatro vírgula doze pontos no capital cognitivo prediz as disparidades de riqueza entre os estados brasileiros.

### Organização de Engenharia
* Os scripts de extração socioeconômica permanecem no diretório `src/preditores/`, mas agora coletam as variáveis que serão explicadas pelo capital cognitivo (Outcomes).
* A validação de convergência de 0.9393 entre ENEM e PISA ganha importância central, pois garante que o preditor principal (IV) é estatisticamente sólido.
* O pipeline de regressão será ajustado para colocar os dados do ENEM no eixo X (causa) e os dados de riqueza no eixo Y (efeito).

---

## [30/12/2025] Reestruturação de diretórios e isolamento de preditores
(Registro anterior mantido para histórico...)

# Devlog: Análise do Capital Cognitivo Brasil

## [30/12/2025] Simplificação da estrutura de indicadores
Ajuste final na organização dos diretórios para refletir a hierarquia de causalidade entre capital cognitivo e indicadores socioeconômicos.

### Mudanças estruturais
* Criação da pasta `src/indicadores/` para armazenar todos os scripts de coleta de variáveis dependentes.
* Os scripts do ENEM e PISA permanecem na raiz de `src/`, estabelecidos como os preditores principais do modelo.
* Padronização dos nomes de arquivos para garantir a rastreabilidade dos dados coletados para o Brasil.

### Lista de indicadores mapeados
* Econômicos: PIB per capita, rendimento domiciliar e Índice de Gini.
* Sociais: IDH, escolaridade dos pais e proficiência em inglês.
* Infraestrutura: Saneamento básico (SNIS) e acesso à internet.
* Educacionais: Fluxo escolar, investimento por aluno e qualificação docente.

### Status da engenharia
* Chave primária de cruzamento: SG_UF_PROVA.
* Destino de salvamento: analise_exploratoria/.
* Validação de consistência: O abismo de sessenta e quatro vírgula doze pontos será a base da análise regressiva contra os indicadores acima.

---
# Devlog: Análise do Capital Cognitivo Brasil

## [30/12/2025] Implementação de protocolo de qualidade (Health check)
Para garantir a confiabilidade do abismo de sessenta e quatro vírgula doze pontos e sua correlação com indicadores do PIB e PISA, foi estabelecido um protocolo rigoroso de validação de dados.

### Protocolo de Validação de Engenharia
* Verificação compulsória de vinte e sete registros (unidades da federação do Brasil).
* Validação da chave primária SG_UF_PROVA em todos os arquivos da pasta `analise_exploratoria/`.
* Teste de integridade de nulos e tipagem de dados (Esquema).
* Verificação de sanidade de valores para as variáveis dependentes e independentes.

### Mudanças no Fluxo de Trabalho
* Todos os scripts de indicadores (pasta `src/indicadores/`) agora devem incluir uma chamada de health check antes de encerrar o processo de salvamento.
* Criada a regra de interrupção do pipeline caso um indicador falhe na validação de completude (menos de vinte e sete UFs).
* O script de consolidado final (05b) passará a emitir um alerta caso os dados estruturais de 2022 apresentem inconsistências com a rigidez de 0.9817 identificada no ranking.

### Status dos Scripts
* 01_extrair_microdados_enem.py: Validado.
* 04_extrair_dados_pisa.py: Validado com os dados da OCDE.
* Scripts de indicadores: Em fase de implementação dos testes unitários de saúde.

---
# Devlog: Análise do Capital Cognitivo Brasil

## [30/12/2025] Execução sequencial dos scripts de indicadores
Iniciada a implementação sistemática dos onze scripts de coleta de variáveis dependentes, seguindo a ordem cronológica do terminal.

### Status da Implementação
* `04_extrair_dados_snis.py`: Concluído com health check de infraestrutura.
* `04_extrair_docentes_inep.py`: Planejado para coletar qualificação pedagógica.
* `04_extrair_pib_capita.py`: Finalizado e validado como métrica de riqueza.
* Protocolo de Qualidade: Aplicado em todas as vinte e sete unidades da federação.

### Notas sobre o Modelo Causal
* O abismo de sessenta e quatro vírgula doze pontos no ENEM é a variável independente que testará a variação nos índices de saneamento, riqueza e proficiência em inglês.
* A rigidez de 0.9817 do W de Kendall justifica o cruzamento de dados estruturais de diferentes fontes sob a mesma hierarquia estadual.

---
# Devlog: Análise do Capital Cognitivo Brasil

## [30/12/2025] Depuração de caminhos e inspeção de arquivos SNIS
Correção de erro de localização de arquivos (FileNotFoundError) e validação de codificação dupla para os arquivos de indicadores.

### Atividades Realizadas
* Ajuste na lógica de caminhos dos scripts de inspeção para utilizar o diretório de trabalho atual (CWD).
* Criação de protocolo de inspeção multienconding (UTF-16LE, Latin-1 e UTF-8) para os arquivos CSV do SNIS.
* Preparação para o mapeamento das colunas de água e esgoto nos arquivos `Consolidado` e `Municípios`.

### Notas de Engenharia
* O arquivo `snis_municipios_2022.csv` apresentou ausência de BOM, exigindo a especificação explícita de UTF-16LE.
* A estrutura de vinte e sete unidades da federação do Brasil permanece como o alvo do health check para os scripts de indicadores.
* A estabilidade do ranking (0.9817) será confrontada com esses dados assim que a extração for concluída.

---
# Devlog: Análise do Capital Cognitivo Brasil

## [30/12/2025] Consolidação final do indicador de saneamento (SNIS)
Finalizada a extração dos dados de infraestrutura básica após resolução de conflitos de codificação e mapeamento de colunas.

### Detalhes Técnicos
* Identificadas as colunas IN055 (atendimento de água) e IN056 (atendimento de esgoto) no arquivo `snis_municipios_2022.csv`.
* O processamento utiliza a codificação UTF-16LE para garantir a leitura correta dos nomes das unidades da federação.
* O Health Check validou a presença das vinte e sete unidades da federação com sucesso.

### Aplicação no Modelo
* O acesso ao saneamento é tratado como variável dependente (Y).
* A análise buscará correlacionar a rigidez de 0.9817 do capital cognitivo com a persistência do déficit hídrico e sanitário em estados com menores notas no ENEM e PISA.

---
# devlog: análise do capital cognitivo brasil

## [30/12/2025] consolidação de indicadores de eficiência escolar
avanço na coleta de variáveis dependentes com foco na qualificação do corpo docente das unidades da federação.

### implementações realizadas
* script `src/indicadores/04_extrair_docentes_inep.py`: implementado com foco na porcentagem de professores com ensino superior.
* protocolo de health check: validado para garantir que todos os vinte e sete registros estejam presentes e dentro do intervalo de zero a cem por cento.
* manutenção da arquitetura: os dados processados foram direcionados para a pasta `analise_exploratoria/` utilizando a chave universal SG_UF_PROVA.

### notas de pesquisa
* a qualificação docente é tratada como uma variável de resposta (Y) ao nível de capital cognitivo (X) do estado.
* o abismo de sessenta e quatro vírgula doze pontos no enem servirá como base para explicar por que estados com maior capital cognitivo conseguem manter corpos docentes mais qualificados.
* a rigidez de zero vírgula nove oito um sete no ranking garante que a relação entre investimento humano e resultado educacional seja estrutural.

---
