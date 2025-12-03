Databricks notebook source
## General Info
| Info | Details |
|-----------|--------|
| Table Name | gold.instagram_gold|
| Source | silver.instagram_silver|
|-----------|--------|
 ## Latest Updates
| Date | Develop by | Reason |
|------|------------|--------|
| 02/12/25 | Samuel Mendes | Notebook Creation |

----------------------------

#Libraries
from pyspark.sql.functions import current_date, current_timestamp, expr

----------------------------
 
#Declaring the schema name of the Table in the catalog
database = 'gold'
table = 'instagram_gold'

----------------------------

df_gold = spark.sql(
f'''
SELECT
  *
FROM
  instagram_data.silver.instagram_silver;''')

----------------------------

instagram_gold = df_gold

instagram_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .clusterBy("post_id") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"instagram_data.gold.{table}")
print("Data loaded successfully!")
