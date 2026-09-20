# CineData Analytics - Rocket Lab 2026.2
O objetivo do projeto é estruturar os dados de um catálogo de filmes na infraestrutura Data Lakehouse utilizando o Databricks. O projeto é realizado com a Arquitetura Medalhão, implementando todo o processo de ETL desde a ingestão dos dados brutos até a geração de Data Marts analíticos.

---
 
## Dataset
 
- **Fonte:** base combinada TMDB/IMDb — 5 arquivos CSV com inconsistências propositais (Column Shift, múltiplos formatos de data, separadores mistos, valores fora de escala)
- **Cotação do Dólar:** API do Banco Central do Brasil (PTAX)
---
 
## Arquitetura
 
O projeto segue a **Arquitetura Medalhão**:
 
```
Landing Zone → Bronze → Silver → Gold
```
 
| Camada | Descrição |
|---|---|
| **Landing** | Arquivos CSV brutos carregados no Volume do Databricks |
| **Bronze** | Dados ingeridos em Delta, sem alteração, com timestamp de ingestão |
| **Silver** | Dados limpos, tipados e com nomes de colunas em português |
| **Gold** | Star Schema para BI + tabela de contexto para RAG |
 
---
 
## Estrutura do Repositório
 
| Arquivo | Descrição |
|---|---|
| `Landing_to_Bronze.ipynb` | Ingestão dos 5 CSVs e da API do Banco Central |
| `Bronze_to_Silver.ipynb` | Limpeza, tipagem, tradução e regras de negócio |
| `Silver_to_Gold.ipynb` | Star Schema, tabela de contexto para IA e desafio de analytics |
| `job.yaml` | Exportação do Workflow do Databricks |
| `print_execucao_job.png` | Print da execução com sucesso do pipeline |
 
---
 
## Camada Bronze
 
Ingestão do dado bruto sem nenhuma transformação. A leitura usa `inferSchema=false` propositalmente: se o Spark inferisse o tipo, converteria silenciosamente valores sujos em nulos e a Silver perderia o que tratar.
 
- Criação do database `bronze`
- Ingestão dos 5 CSVs como Delta Tables em modo **Append**
- Coluna `ingestion_datetime` em cada tabela, com o timestamp da carga
- Extração da cotação do dólar via API do BACEN, com janela de 7 dias corridos (garante ao menos um dia útil, já que a API não publica em fins de semana e feriados)
**Tabelas geradas:** `tb_movies_info`, `tb_movies_financials`, `tb_movies_metrics`, `tb_credits_and_tags`, `tb_movies_reviews`, `tb_cotacao_dolar`
 
---
 
## Camada Silver
 
Nenhuma tabela Bronze é alterada — elas são apenas lidas. Toda a limpeza acontece aqui.
 
**Tabelas geradas:** `tb_info_filmes`, `tb_financeiro_filmes`, `tb_metricas_engajamento`, `tb_avaliacoes_usuarios`, `tb_generos`, `tb_pessoas_empresas`, `tb_cotacao_dolar`
 
### Principais regras de negócio
 
*(os notebooks trazem os comentários detalhados de cada decisão)*
 
**Deduplicação por ingestão** — como a Bronze grava em Append, cada reexecução duplica registros. A deduplicação usa `Window` + `row_number()` particionado por filme e ordenado por `ingestion_datetime` decrescente, mantendo a versão mais recente.
 
**Conversão segura de tipo** — o Column Shift espalhou textos pelas colunas numéricas (nomes de diretores em campos de nota, idiomas em contagens de votos). O `try_cast` devolve NULL nesses casos em vez de interromper a execução, diferente do `cast` comum.
 
**Datas multi-formato** — a origem mistura três padrões (`yyyy-MM-dd`, `MM-dd-yyyy`, `dd/MM/yyyy`). Um `coalesce` encadeando `try_to_date` testa um por vez; só vira NULL quando nenhum funciona.
 
**Normalização antes da tradução** — a coluna de status tinha 16 variações da mesma informação (`Released`, `RELEASED`, `In-Production`). A padronização de caixa e remoção de hífens acontece antes do mapeamento, de modo que o dicionário precise de uma entrada por status e não por variação.
 
**Higienização monetária com expansão de sufixo** — valores vêm como `97000000`, `$ 97000000`, `USD 10000` e `34.0M`. O sufixo K/M/B é capturado **antes** de ser removido, para virar multiplicador; do contrário `34.0M` seria lido como 34 dólares.
 
**Ausência vs. zero** — textos como `Unknown` e `Não Informado` viram NULL antes da conversão. Valores zerados e negativos também são tratados como ausentes: na base de origem, zero significa "não informado", não "não faturou".
 
**Separador decimal vs. separador de milhar** — na popularidade a vírgula é separador **decimal** (`154,34`), então é substituída por ponto, nunca removida. Nas colunas monetárias, a mesma vírgula seria separador de milhar — regras opostas para o mesmo caractere.
 
**Limites de negócio** — notas fora do intervalo 0–10 são invalidadas, incluindo cerca de 3 mil registros em escala 0–100. Contagens de votos e popularidade negativas também viram NULL.
 
**Explode com separadores mistos** — a coluna de gêneros usa vírgula, ponto-e-vírgula **e pipe**. Os três são normalizados antes do `split`, e o resultado é validado contra o domínio fechado de 19 gêneros, o que elimina cerca de 17 mil resíduos (países, idiomas, números e caminhos de imagem deslocados).
 
**Dimensão unificada de entidades** — `cast`, `directors`, `writers` e `production_companies` são consolidadas numa tabela única categorizada por tipo (Ator, Diretor, Roteirista, Produtora), com capitalização padronizada e remoção de resíduos.
 
**Forward Fill na cotação** — a API do BACEN não publica em fins de semana e feriados. Um calendário contínuo é gerado e cada dia sem cotação herda o valor do último dia útil, via `last(..., ignorenulls=True)` sobre janela ordenada.
 
---
 
## Camada Gold
 
### Entrega 1 — Modelagem Dimensional (Star Schema)
 
**Fato:** `fact_movies_performance` — grão de um registro por filme lançado, consolidando métricas financeiras e de engajamento.
 
**Dimensões:** `dim_movies`, `dim_genres`, `dim_people`, `dim_companies`, `dim_reviews`
 
**Bridges:** `bridge_movie_genre`, `bridge_movie_person`, `bridge_movie_company`
 
**Por que bridge tables:** um filme tem vários gêneros, atores e produtoras. Um join direto na fato multiplicaria o registro do filme — um título com 5 gêneros teria a receita contada 5 vezes em qualquer soma. As bridges isolam esses relacionamentos muitos-para-muitos e preservam o grão da fato.
 
**Estratégia de Surrogate Key:** `monotonically_increasing_id()`, escolhido por gerar BIGINT direto (tipo exigido) e não precisar de shuffle, diferente do `row_number()` com janela global. Como a função não é determinística, cada dimensão é **gravada antes** e **relida do disco** para montar fato e bridges — caso contrário o Spark poderia recalcular as chaves durante o join e as FKs apontariam para registros errados.
 
### Entrega 2 — Tabela de contexto para IA
 
`gold_genai_movies_context` — um documento em texto corrido por filme, pronto para vetorização no Vector Search.
 
**O tratamento de nulos:** `concat()` retorna NULL para a string inteira se qualquer campo for nulo. Como a receita está ausente em ~97% dos filmes e a sinopse em ~14%, uma concatenação ingênua reduziria a tabela de 97 mil para pouco mais de 2 mil registros, sem gerar erro algum. Cada campo passa por `coalesce()` com um fallback textual antes da concatenação.
 
Os fallbacks foram escolhidos pelo sentido semântico, não só pelo efeito técnico: receita ausente vira "valor não informado" e nunca zero, porque zero afirmaria que o filme não faturou nada — um erro factual que induziria o modelo de linguagem ao erro.
 
### Desafio de Analytics
 
As 6 perguntas de negócio estão resolvidas no notebook `Silver_to_Gold.ipynb`, com `display()`. A data de corte das perguntas 5 e 6 é calculada a partir do dado (lançamento mais recente já realizado), não fixada manualmente.
 
---
 
## Orquestração
 
Pipeline orquestrado via **Databricks Workflows**:
 
- 3 tasks sequenciais: `to_Bronze → to_Silver → to_Gold`
- Dependências explícitas — cada task só inicia após a conclusão da anterior
- Agendamento diário às **10:00 (America/Fortaleza)**
---
 
## Tecnologias
 
- **Databricks** — plataforma de processamento
- **Apache Spark / PySpark** — processamento distribuído
- **Delta Lake** — formato de armazenamento com ACID
- **Python** e **SQL** — linguagens dos notebooks
