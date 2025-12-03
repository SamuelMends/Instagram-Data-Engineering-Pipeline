# Databricks notebook source
# MAGIC %md
# MAGIC ## General Info
# MAGIC | Info | Details |
# MAGIC |-----------|--------|
# MAGIC | Table Name | gold.instagram_gold|
# MAGIC | Source | silver.instagram_silver|
# MAGIC
# MAGIC ## Latest Updates
# MAGIC | Date | Develop by | Reason |
# MAGIC |------|------------|--------|
# MAGIC | 02/12/25 | Samuel Mendes | Notebook Creation |

# COMMAND ----------

#Libraries
from pyspark.sql.functions import current_date, current_timestamp, expr

# COMMAND ----------

#Declaring the schema name of the Table in the catalog
database = 'gold'
table = 'instagram_gold'

# COMMAND ----------

df_gold = spark.sql(
f'''
SELECT
  *
FROM
  instagram_data.silver.instagram_silver;''')

# COMMAND ----------

instagram_gold = df_gold

instagram_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .clusterBy("post_id") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"instagram_data.gold.{table}")
print("Data loaded successfully!")
