# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — ingestão incremental e idempotente (requisito 05)
# MAGIC Auto Loader (`cloudFiles`) com `availableNow`: lê **só os arquivos novos** do Volume. O *checkpoint* guarda o que já foi lido,
# MAGIC então rodar de novo não duplica nada. Cada linha guarda o arquivo de origem e o momento da carga.
# MAGIC
# MAGIC **Roteiro para o print do relatório:** rode a célula 2 (contagem) → suba o lote 2 → rode as células 1 e 2 → rode as células 1 e 2 outra vez.

# COMMAND ----------

from pyspark.sql import functions as F

BASE = "/Volumes/workspace/gs/chegada"


def ingerir(fonte: str, multiline: bool):
    leitor = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{BASE}/_controle/schema_{fonte}")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", str(multiline).lower())   # relatos = array JSON; clima = objeto JSON
    )
    return (
        leitor.load(f"{BASE}/{fonte}")
        .withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("carregado_em", F.current_timestamp())
        .writeStream.option("checkpointLocation", f"{BASE}/_controle/checkpoint_{fonte}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"workspace.gs.bronze_{fonte}")
    )


# COMMAND ----------

# 1) Executa a ingestão incremental das duas fontes
for fonte, multi in (("clima", True), ("relatos", True)):
    ingerir(fonte, multi).awaitTermination()

# COMMAND ----------

# 2) Contagens (cole o resultado no relatório: antes / depois do 2º lote / depois da 2ª execução)
spark.sql("""
    SELECT 'bronze_clima'   AS tabela, COUNT(*) AS linhas, COUNT(DISTINCT arquivo_origem) AS arquivos FROM workspace.gs.bronze_clima
    UNION ALL
    SELECT 'bronze_relatos' AS tabela, COUNT(*) AS linhas, COUNT(DISTINCT arquivo_origem) AS arquivos FROM workspace.gs.bronze_relatos
""").show()

# COMMAND ----------

# 3) Arquivos já carregados (prova de que o 2º lote só trouxe arquivos novos)
spark.sql("""
    SELECT arquivo_origem, COUNT(*) AS linhas, MIN(carregado_em) AS carregado_em
    FROM workspace.gs.bronze_relatos GROUP BY arquivo_origem ORDER BY arquivo_origem
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Para reprocessar do zero (só em testes)
# MAGIC ```sql
# MAGIC DROP TABLE IF EXISTS workspace.gs.bronze_clima; DROP TABLE IF EXISTS workspace.gs.bronze_relatos;
# MAGIC ```
# MAGIC e apague as pastas `_controle/checkpoint_*` e `_controle/schema_*` do Volume.
