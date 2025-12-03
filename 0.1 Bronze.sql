-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Bronze Layer

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Libraries
-- MAGIC from pyspark.sql.functions import current_date, expr
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Declaring the database and table names:
-- MAGIC database = 'bronze'
-- MAGIC table = 'instagram_bronze'
-- MAGIC path = '/Volumes/instagram_data/bronze/instagram_data/Instagram_Analytics.csv'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Adding variable path to the data frame
-- MAGIC df = spark.read.format("csv").option("header", True).load(path)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Adding two columns for better management and data governance
-- MAGIC df = df.withColumn("load_date", current_date())
-- MAGIC df = df.withColumn("load_date_time", expr("current_timestamp() - INTERVAL 3 HOURS"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write \
-- MAGIC     .format('delta') \
-- MAGIC     .mode('overwrite') \
-- MAGIC     .option('mergeSchema', 'true') \
-- MAGIC     .option('overwriteSchema', 'true') \
-- MAGIC     .saveAsTable(f"instagram_data.{database}.{table}")
-- MAGIC print("Data loaded successfully!")