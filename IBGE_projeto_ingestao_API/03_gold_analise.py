# Databricks notebook source
# MAGIC %md
# MAGIC **ANÁLISE DOS DADOS**

# COMMAND ----------

#Ler a tabela Silver
df = spark.table("silver_estados")

# COMMAND ----------

#Estados por Região
df.groupBy("regiao_nome").count().show()

# COMMAND ----------

#Ordenar
df.orderBy("estado").show()

# COMMAND ----------

#
spark.sql("""
SELECT regiao_nome, COUNT(*) as total
FROM silver_estados
GROUP BY regiao_nome
ORDER BY total DESC
""").show()