# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Landing to Bronze — CineData Analytics
# MAGIC
# MAGIC **Regra da camada Bronze:** nenhuma limpeza, nenhuma conversão de tipo, nenhuma
# MAGIC renomeação. O dado entra exatamente como veio do arquivo de origem.
# MAGIC A única coluna adicionada é `ingestion_datetime`, que registra o momento da carga
# MAGIC (essa coluna é o que permite, na Silver, deduplicar mantendo a versão mais recente).
# MAGIC
# MAGIC Fontes: 5 arquivos CSV no Volume + API de cotação do dólar (PTAX / Banco Central).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parâmetros do notebook (widgets)
# MAGIC As datas da API ficam como parâmetro para que o Job possa rodar com qualquer janela.
# MAGIC Deixando em branco, o notebook assume os últimos 7 dias corridos.

# COMMAND ----------

dbutils.widgets.text("data_inicio", "", "Data inicial (MM-DD-AAAA)")
dbutils.widgets.text("data_fim", "", "Data final (MM-DD-AAAA)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuração e criação do database Bronze

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from datetime import date, timedelta
import requests
import pandas as pd

# Ajuste o catálogo e o caminho do volume conforme o seu workspace.
CATALOG = "workspace"
VOLUME = f"/Volumes/{CATALOG}/default/landing"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

print(f"Catálogo: {CATALOG}")
print("Arquivos encontrados no volume:")
for arquivo in dbutils.fs.ls(VOLUME):
    print(" -", arquivo.name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingestão dos arquivos CSV
# MAGIC
# MAGIC Pontos importantes da leitura:
# MAGIC - `inferSchema = false`: tudo entra como STRING. Se deixássemos o Spark inferir tipo,
# MAGIC   ele converteria silenciosamente valores sujos para NULL — a sujeira precisa chegar
# MAGIC   intacta na Bronze para ser tratada na Silver.
# MAGIC - `multiLine` e `escape`: os comentários das reviews podem conter vírgulas e quebras
# MAGIC   de linha dentro das aspas.
# MAGIC - `mode("append")`: exigência do escopo, permite histórico de cargas.

# COMMAND ----------

arquivos_para_tabelas = {
    # Atenção: o arquivo real vem como TMDB_IMDB, invertido em relação ao que o escopo escreveu.
    "movies_info_TMDB_IMDB.csv":       "bronze.tb_movies_info",
    "movies_financials_IMDB_TMDB.csv": "bronze.tb_movies_financials",
    "movies_metrics_IMDB_TMDB.csv":    "bronze.tb_movies_metrics",
    "credits_and_tags_IMDB_TMDB.csv":  "bronze.tb_credits_and_tags",
    "movies_reviews.csv":              "bronze.tb_movies_reviews",
}

# COMMAND ----------

for arquivo, tabela in arquivos_para_tabelas.items():

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .load(f"{VOLUME}/{arquivo}")
    )

    # Carimbo de ingestão: timestamp exato do momento da inserção na Bronze.
    df = df.withColumn("ingestion_datetime", current_timestamp())

    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(tabela)
    )

    print(f"{arquivo:<35} -> {tabela:<32} | {df.count():>6} linhas | {len(df.columns)} colunas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ingestão de API — cotação do dólar (PTAX / Banco Central)
# MAGIC
# MAGIC A API não retorna cotação em fins de semana e feriados. Por isso a janela padrão é de
# MAGIC 7 dias corridos: garante pelo menos um dia útil no período. O preenchimento dos dias
# MAGIC faltantes (forward fill) é responsabilidade da camada Silver, não da Bronze.

# COMMAND ----------

hoje = date.today()

data_inicio_formatada = dbutils.widgets.get("data_inicio").strip() or (hoje - timedelta(days=7)).strftime("%m-%d-%Y")
data_fim_formatada = dbutils.widgets.get("data_fim").strip() or hoje.strftime("%m-%d-%Y")

print(f"Período consultado: {data_inicio_formatada} até {data_fim_formatada}")

url = (
    "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
    "CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)"
    f"?@dataInicial='{data_inicio_formatada}'"
    f"&@dataFinalCotacao='{data_fim_formatada}'"
    "&$select=dataHoraCotacao,cotacaoCompra"
    "&$format=json"
)

resposta = requests.get(url, timeout=60)
resposta.raise_for_status()

cotacoes = resposta.json().get("value", [])
print(f"{len(cotacoes)} cotações retornadas pela API")

# COMMAND ----------

if not cotacoes:
    raise ValueError(
        "A API não retornou nenhuma cotação no período. "
        "Amplie o intervalo de datas — provavelmente a janela caiu inteira em fim de semana/feriado."
    )

df_cotacao = spark.createDataFrame(pd.DataFrame(cotacoes))
df_cotacao = df_cotacao.withColumn("ingestion_datetime", current_timestamp())

(
    df_cotacao.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("bronze.tb_cotacao_dolar")
)

display(df_cotacao)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validação da camada

# COMMAND ----------

display(spark.sql("SHOW TABLES IN bronze"))

# COMMAND ----------

for tabela in list(arquivos_para_tabelas.values()) + ["bronze.tb_cotacao_dolar"]:
    print(f"\n=== {tabela} ===")
    spark.table(tabela).printSchema()
    display(spark.table(tabela).limit(5))