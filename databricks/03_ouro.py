# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Ouro — modelo dimensional + LGPD (requisitos 07 e 08)
# MAGIC **Fato:** `fato_relato` (1 linha por relato) e `fato_clima_dia`.  **Dimensões:** `dim_data`, `dim_local`, `dim_relator`.
# MAGIC
# MAGIC **LGPD na Ouro (minimização + pseudonimização):**
# MAGIC * `id_relator` (identificador do aparelho) → **hash SHA-256 com sal**; o valor original nunca chega à Ouro.
# MAGIC * Coordenadas → **arredondadas a 3 casas (~110 m)** em `dim_local`; a Ouro não guarda lat/lon exata do relato.
# MAGIC * O sal vem de um *widget* preenchido na execução (não fica no repositório).
# MAGIC
# MAGIC As tabelas são recriadas por `CREATE OR REPLACE` (idempotente).

# COMMAND ----------

dbutils.widgets.text("sal", "")
SAL = dbutils.widgets.get("sal")
assert SAL, "Preencha o widget 'sal' com um texto secreto (não o salve no repositório)."

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gs.dim_data AS
# MAGIC SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS sk_data, d AS data, year(d) AS ano, month(d) AS mes, day(d) AS dia,
# MAGIC        date_format(d, 'EEEE') AS dia_semana, dayofweek(d) IN (1, 7) AS fim_de_semana
# MAGIC FROM (SELECT explode(sequence(DATE'2026-10-01', DATE'2026-12-31', INTERVAL 1 DAY)) AS d);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- dim_local: célula de ~110 m. Relatos da mesma célula são o "mesmo buraco" (deduplicação geográfica do MVP).
# MAGIC CREATE OR REPLACE TABLE workspace.gs.dim_local AS
# MAGIC SELECT concat(cast(round(lat, 3) AS STRING), '_', cast(round(lon, 3) AS STRING)) AS id_local,
# MAGIC        round(lat, 3) AS lat_aprox, round(lon, 3) AS lon_aprox,
# MAGIC        min(via) AS via, 'Bela Vista' AS bairro, 'São Paulo' AS cidade
# MAGIC FROM workspace.gs.prata_relatos
# MAGIC GROUP BY round(lat, 3), round(lon, 3);

# COMMAND ----------

# dim_relator: apenas o hash (pseudônimo). Sem o sal, não dá para voltar ao identificador original.
from pyspark.sql import functions as F, Window

p = spark.table("workspace.gs.prata_relatos").withColumn(
    "id_relator_hash", F.sha2(F.concat(F.lit(SAL), F.col("id_relator")), 256))
(p.groupBy("id_relator_hash")
   .agg(F.count("*").alias("total_relatos"), F.min("criado_em").alias("primeiro_relato_em"))
   .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gs.dim_relator"))

# COMMAND ----------

# fato_relato: sem lat/lon exata, sem id_relator original
fato = (
    p.withColumn("sk_data", F.date_format("criado_em", "yyyyMMdd").cast("int"))
     .withColumn("id_local", F.concat_ws("_", F.round("lat", 3).cast("string"), F.round("lon", 3).cast("string")))
     .withColumn("horas_ate_atualizacao", (F.col("atualizado_em").cast("long") - F.col("criado_em").cast("long")) / 3600.0)
     .withColumn("ordem_no_local", F.row_number().over(Window.partitionBy("id_local").orderBy("criado_em")))
     .withColumn("duplicado", F.col("ordem_no_local") > 1)       # 2º relato em diante do mesmo buraco
     .select("id_relato", "sk_data", "id_local", "id_relator_hash", "severidade", "status", "origem", "confianca",
             "criado_em", "atualizado_em", "horas_ate_atualizacao", "duplicado", "origem_dado")
)
fato.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gs.fato_relato")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gs.fato_clima_dia AS
# MAGIC SELECT CAST(date_format(data_hora, 'yyyyMMdd') AS INT) AS sk_data,
# MAGIC        round(sum(chuva_mm), 1) AS chuva_mm, round(avg(temperatura_c), 1) AS temp_media_c,
# MAGIC        round(max(temperatura_c), 1) AS temp_max_c, count(*) AS horas_validas
# MAGIC FROM workspace.gs.prata_clima GROUP BY CAST(date_format(data_hora, 'yyyyMMdd') AS INT);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contrato com o MVP (requisito 08)
# MAGIC A tela **Fila da zeladoria** consome a view `workspace.gs.vw_mvp_fila_zeladoria` (1 linha por buraco/célula), exportada como JSON
# MAGIC em `/Volumes/workspace/gs/chegada/saida/fila_zeladoria.json`. Campos e regras em `docs/contrato_mvp.md`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.gs.vw_mvp_fila_zeladoria AS
# MAGIC SELECT l.id_local, l.via, l.bairro, l.lat_aprox, l.lon_aprox,
# MAGIC        count(*) AS relatos,
# MAGIC        count(DISTINCT f.id_relator_hash) AS relatores_distintos,
# MAGIC        max(CASE f.severidade WHEN 'alta' THEN 3 WHEN 'media' THEN 2 ELSE 1 END) AS severidade_max,
# MAGIC        max(f.criado_em) AS ultimo_relato_em,
# MAGIC        min_by(f.status, CASE f.status WHEN 'pendente' THEN 0 WHEN 'confirmado' THEN 1 ELSE 2 END) AS status_prioritario
# MAGIC FROM workspace.gs.fato_relato f JOIN workspace.gs.dim_local l USING (id_local)
# MAGIC WHERE f.status <> 'descartado'
# MAGIC GROUP BY l.id_local, l.via, l.bairro, l.lat_aprox, l.lon_aprox;

# COMMAND ----------

import json
linhas = [r.asDict(recursive=True) for r in spark.table("workspace.gs.vw_mvp_fila_zeladoria")
          .orderBy(F.desc("severidade_max"), F.desc("relatos")).collect()]
for r in linhas:
    r["ultimo_relato_em"] = r["ultimo_relato_em"].isoformat()
dbutils.fs.mkdirs("/Volumes/workspace/gs/chegada/saida")
dbutils.fs.put("/Volumes/workspace/gs/chegada/saida/fila_zeladoria.json", json.dumps(linhas, ensure_ascii=False), True)
print(len(linhas), "buracos exportados")
