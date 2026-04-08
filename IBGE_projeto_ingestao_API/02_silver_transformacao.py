# Databricks notebook source
# MAGIC %md
# MAGIC **TRATAMENTO DOS DADOS**

# COMMAND ----------

#Ler a tabela Bronze
df = spark.table("bronze_estados")

# COMMAND ----------

#Ver estrutura
df.printSchema()

# COMMAND ----------

#Tratar os dados

from pyspark.sql.functions import col

df_silver = df.select(
    col("id").alias("id_estado"),
    col("sigla"),
    col("nome").alias("estado"),
    col("regiao.id").alias("regiao_id"),
    col("regiao.sigla").alias("regiao_sigla"),
    col("regiao.nome").alias("regiao_nome")
)

# COMMAND ----------

# Visualizar os dados
df_silver.show()

# COMMAND ----------

#Salvar como tabela(Silver)
df_silver.write.mode("overwrite").saveAsTable("silver_estados")