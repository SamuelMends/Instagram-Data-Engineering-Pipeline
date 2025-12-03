-- Databricks notebook source
## Bronze Layer

%python
#Libraries
from pyspark.sql.functions import current_date, expr

--------------------------------------------------------------

#Declaring the database and table names:
database = 'bronze'
table = 'instagram_bronze'
path = '/Volumes/instagram_data/bronze/instagram_data/Instagram_Analytics.csv'


#Adding variable path to the data frame
df = spark.read.format("csv").option("header", True).load(path)

-------------------------------
df.display()
------------------------------

# Adding two columns for better management and data governance
df = df.withColumn("load_date", current_date())
df = df.withColumn("load_date_time", expr("current_timestamp() - INTERVAL 3 HOURS"))

df.display()

df.write \
format('delta') \
.mode('overwrite') \
.option('mergeSchema', 'true') \
.option('overwriteSchema', 'true') \
.saveAsTable(f"instagram_data.{database}.{table}")
print("Data loaded successfully!")
