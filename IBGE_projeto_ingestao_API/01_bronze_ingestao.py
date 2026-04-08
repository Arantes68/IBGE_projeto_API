# Databricks notebook source
# MAGIC %md
# MAGIC **INGESTÃO DOS DADOS**

# COMMAND ----------

# Importar Biblioteca
import pandas as pd

# COMMAND ----------

# Importar Biblioteca
import requests
import json

# Requisição
url = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"

response = requests.get(url)

print(response.status_code)

# Descrição de erros
if response.status_code == 200:
    print("Requisição com sucesso")
else:
    print("Erro na requisição")


# COMMAND ----------

#Converter JSON
dados = response.json()

print(len(dados))
print(dados[0])

# COMMAND ----------

#Converter para Dataframe
df_pandas = pd.DataFrame(dados)
df_pandas.head()

# COMMAND ----------

#Converter para Spark
df_spark = spark.createDataFrame(df_pandas)

# COMMAND ----------

df_spark.head(20)

# COMMAND ----------

#Salvar como tabela(Bronze)
df_spark.write.mode("overwrite").saveAsTable("bronze_estados")