# Databricks notebook source
# MAGIC %md
# MAGIC ## General Info
# MAGIC | Info | Details |
# MAGIC |-----------|--------|
# MAGIC | Table Name | silver.instagram_silver|
# MAGIC | Source | bronze.instagram_bronze|
# MAGIC
# MAGIC ## Latest Updates
# MAGIC | Date | Develop by | Reason |
# MAGIC |------|------------|--------|
# MAGIC | 02/12/25 | Samuel Mendes | Notebook Creation |

# COMMAND ----------

#Librabries
from pyspark.sql.functions import current_date, current_timestamp, expr, col, to_timestamp, to_date, date_format

# COMMAND ----------

# MAGIC %md
# MAGIC #### Following the same line as bronze, we need to declare the name of the Database and the name of the table in the catalog, but this time we don't need the file path:

# COMMAND ----------

#Declaring names:
database = 'silver'
table = 'instagram_silver'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     *
# MAGIC FROM
# MAGIC   instagram_data.bronze.instagram_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Time to define the type of our columns

# COMMAND ----------

df_instagram_data_tst = spark.sql(
    """
    SELECT
  CAST(post_id AS STRING),
  CAST(upload_date AS STRING),
  CAST(media_type AS STRING),
  CAST(likes AS INT),
  CAST(comments AS INT),
  CAST(shares AS INT),
  CAST(saves AS INT),
  CAST(reach AS INT),
  CAST(impressions AS INT),
  CAST(caption_length AS INT),
  CAST(hashtags_count AS INT),
  CAST(followers_gained AS INT),
  CAST(traffic_source AS STRING),
  CAST(engagement_rate AS DECIMAL(20,2)),
  CAST(content_category AS STRING)
FROM instagram_data.bronze.instagram_bronze
    """
)
display(df_instagram_data_tst)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Converting Upload_Date into a Proper Timestamp

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

df_instagram_data_tst = df_instagram_data_tst.withColumn(
    "upload_date_ts",
    to_timestamp(col("upload_date"), "yyyy-MM-dd HH:mm:ss.SSSSSS")
)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Tiny Adjustment on the Upload_Date column

# COMMAND ----------

df_instagram_data_tst.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Extracting Date and Time Columns

# COMMAND ----------

#Splitting timestamp into separate, analytics-friendly fields
from pyspark.sql.functions import col, to_timestamp, date_format

df_instagram_data_tst = df_instagram_data_tst.withColumn("Date", date_format(col("upload_date_ts"), "yyyy-MM-dd")) \
       .withColumn("Time", date_format(col("upload_date_ts"), "HH:mm:ss"))



# COMMAND ----------

# Cleaning the table:

df_instagram_data_tst = df_instagram_data_tst.drop(
    "upload_date",
    "upload_date_clean",
    "upload_date_ts"
)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Reordering Columns

# COMMAND ----------

first_col = df_instagram_data_tst.columns[0]

df_instagram_data_tst = df_instagram_data_tst.select(
    first_col,
    "Date",
    "Time",
    *[c for c in df_instagram_data_tst.columns if c not in (first_col, "Date", "Time")]
)


# COMMAND ----------

df_instagram_data_tst.display()

# COMMAND ----------

# Adding two columns for better management and data governance
from pyspark.sql.functions import current_date, current_timestamp, expr
df_instagram_data_tst = df_instagram_data_tst.withColumn("load_date", current_date())
df_instagram_data_tst = df_instagram_data_tst.withColumn("load_date_time", expr("current_timestamp() - INTERVAL 3 HOURS"))

# COMMAND ----------

df_instagram = df_instagram_data_tst

df_instagram.display()

# COMMAND ----------

df_instagram.write \
    .format("delta") \
    .mode("overwrite") \
    .clusterBy("post_id") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"instagram_data.silver.{table}")
print("Data loaded successfully!")