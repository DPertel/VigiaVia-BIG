# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Prata — tipos, nulos/inválidos, MERGE e regras de qualidade (requisito 06)
# MAGIC * **Tipos corretos**: datas como `timestamp`, coordenadas como `double`, texto normalizado.
# MAGIC * **Nulos e inválidos**: tratados por regras; o que reprova vai para `quarentena_*` (nada é apagado em silêncio).
# MAGIC * **MERGE pela chave**: clima = (cidade, data_hora); relatos = id_relato (atualização de status entra por cima do relato antigo).
# MAGIC * **Contagem por regra**: registrada em `workspace.gs.prata_qualidade_log` a cada execução.

# COMMAND ----------

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

CIDADE = "São Paulo - Bela Vista"
agora = F.current_timestamp()

spark.sql("""CREATE TABLE IF NOT EXISTS workspace.gs.prata_qualidade_log
             (tabela STRING, regra STRING, descricao STRING, total_barrado BIGINT, total_avaliado BIGINT, executado_em TIMESTAMP)""")


def registrar(tabela, regras, df_flags):
    """regras: lista de (codigo, descricao, coluna_flag_reprovado). Grava e imprime a contagem barrada por regra."""
    total = df_flags.count()
    linhas = [(tabela, c, d, df_flags.filter(F.col(flag)).count(), total) for c, d, flag in regras]
    log = spark.createDataFrame(linhas, "tabela string, regra string, descricao string, total_barrado long, total_avaliado long") \
        .withColumn("executado_em", agora)
    log.write.mode("append").saveAsTable("workspace.gs.prata_qualidade_log")
    log.drop("executado_em").show(truncate=False)


# COMMAND ----------

# MAGIC %md ## Clima (Open-Meteo): listas paralelas → uma linha por hora

# COMMAND ----------

bz = spark.table("workspace.gs.bronze_clima")
clima = (
    bz.select("arquivo_origem", "carregado_em",
              F.explode(F.arrays_zip("hourly.time", "hourly.temperature_2m", "hourly.precipitation")).alias("h"))
    .select(
        F.lit(CIDADE).alias("cidade"),
        F.to_timestamp("h.time").alias("data_hora"),
        F.col("h.temperature_2m").cast("double").alias("temperatura_c"),
        F.col("h.precipitation").cast("double").alias("chuva_mm"),
        "arquivo_origem", "carregado_em",
    )
)

clima_f = (
    clima
    .withColumn("rep_q1", F.col("data_hora").isNull())                                           # Q1 data/hora válida
    .withColumn("rep_q2", F.col("temperatura_c").isNull() | ~F.col("temperatura_c").between(-10, 50))  # Q2 temperatura plausível
    .withColumn("rep_q3", F.col("chuva_mm").isNull() | (F.col("chuva_mm") < 0))                  # Q3 chuva não nula e >= 0
    .withColumn("reprovado", F.col("rep_q1") | F.col("rep_q2") | F.col("rep_q3"))
)
registrar("prata_clima", [
    ("Q1", "data_hora nula ou inválida", "rep_q1"),
    ("Q2", "temperatura nula ou fora de -10..50 °C", "rep_q2"),
    ("Q3", "precipitação nula ou negativa", "rep_q3"),
], clima_f)

(clima_f.filter("reprovado").drop("rep_q1", "rep_q2", "rep_q3", "reprovado")
    .write.mode("append").option("mergeSchema", "true").saveAsTable("workspace.gs.quarentena_clima"))

# mantém a linha mais recente por chave dentro do lote (evita "MERGE com várias origens")
w = Window.partitionBy("cidade", "data_hora").orderBy(F.col("carregado_em").desc())
clima_ok = (clima_f.filter(~F.col("reprovado")).withColumn("rn", F.row_number().over(w)).filter("rn = 1")
            .select("cidade", "data_hora", "temperatura_c", "chuva_mm", "arquivo_origem", "carregado_em"))

spark.sql("""CREATE TABLE IF NOT EXISTS workspace.gs.prata_clima
             (cidade STRING, data_hora TIMESTAMP, temperatura_c DOUBLE, chuva_mm DOUBLE, arquivo_origem STRING, carregado_em TIMESTAMP)""")
clima_ok.createOrReplaceTempView("lote_prata_clima")
spark.sql("""
    MERGE INTO workspace.gs.prata_clima AS alvo
    USING lote_prata_clima AS novo
    ON alvo.cidade = novo.cidade AND alvo.data_hora = novo.data_hora
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# MAGIC %md ## Relatos do VigiaVia

# COMMAND ----------

bz = spark.table("workspace.gs.bronze_relatos")
relatos = (
    bz.select(
        F.col("id_relato").cast("string"),
        F.col("id_relator").cast("string"),
        F.col("lat").cast("double").alias("lat"),
        F.col("lon").cast("double").alias("lon"),
        F.trim("via").alias("via"),
        F.lower(F.trim("severidade")).alias("severidade"),
        F.col("confianca").cast("double").alias("confianca"),
        F.lower(F.trim("origem")).alias("origem"),
        F.lower(F.trim("status")).alias("status"),
        F.to_timestamp("criado_em").alias("criado_em"),
        F.to_timestamp("atualizado_em").alias("atualizado_em"),
        F.col("origem_dado").cast("string").alias("origem_dado"),
        "arquivo_origem", "carregado_em",
    )
)

relatos_f = (
    relatos
    .withColumn("rep_q1", F.col("id_relato").isNull() | F.col("criado_em").isNull())                       # Q1 chave e data
    .withColumn("rep_q2", F.col("lat").isNull() | F.col("lon").isNull()
                | ~F.col("lat").between(-23.80, -23.30) | ~F.col("lon").between(-46.90, -46.30))            # Q2 coordenada dentro de SP
    .withColumn("rep_q3", ~F.col("severidade").isin("baixa", "media", "alta") | F.col("severidade").isNull())  # Q3 severidade válida
    .withColumn("rep_q4", ~F.col("status").isin("pendente", "confirmado", "descartado") | F.col("status").isNull())  # Q4 status válido
    .withColumn("reprovado", F.col("rep_q1") | F.col("rep_q2") | F.col("rep_q3") | F.col("rep_q4"))
)
registrar("prata_relatos", [
    ("Q1", "id_relato ou criado_em nulo", "rep_q1"),
    ("Q2", "coordenada nula ou fora da região de SP", "rep_q2"),
    ("Q3", "severidade fora de baixa/media/alta", "rep_q3"),
    ("Q4", "status fora de pendente/confirmado/descartado", "rep_q4"),
], relatos_f)

(relatos_f.filter("reprovado").drop("rep_q1", "rep_q2", "rep_q3", "rep_q4", "reprovado")
    .write.mode("append").option("mergeSchema", "true").saveAsTable("workspace.gs.quarentena_relatos"))

# duplicados exatos dentro do lote e múltiplas versões do mesmo id: fica a versão mais recente
w = Window.partitionBy("id_relato").orderBy(F.col("atualizado_em").desc(), F.col("carregado_em").desc())
relatos_ok = (relatos_f.filter(~F.col("reprovado")).withColumn("rn", F.row_number().over(w)).filter("rn = 1")
              .drop("rep_q1", "rep_q2", "rep_q3", "rep_q4", "reprovado", "rn"))

spark.sql("""CREATE TABLE IF NOT EXISTS workspace.gs.prata_relatos
             (id_relato STRING, id_relator STRING, lat DOUBLE, lon DOUBLE, via STRING, severidade STRING, confianca DOUBLE,
              origem STRING, status STRING, criado_em TIMESTAMP, atualizado_em TIMESTAMP, origem_dado STRING,
              arquivo_origem STRING, carregado_em TIMESTAMP)""")
relatos_ok.createOrReplaceTempView("lote_prata_relatos")
spark.sql("""
    MERGE INTO workspace.gs.prata_relatos AS alvo
    USING lote_prata_relatos AS novo
    ON alvo.id_relato = novo.id_relato
    WHEN MATCHED AND novo.atualizado_em >= alvo.atualizado_em THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# Conferência para o print: Prata não tem chave repetida
spark.sql("""
    SELECT 'prata_clima' t, COUNT(*) linhas, COUNT(DISTINCT cidade, data_hora) chaves FROM workspace.gs.prata_clima
    UNION ALL
    SELECT 'prata_relatos', COUNT(*), COUNT(DISTINCT id_relato) FROM workspace.gs.prata_relatos
""").show()
