# Databricks notebook source
from pyspark.sql.functions import*
from pyspark.sql.types import *
from functools import reduce
from pyspark.sql import DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC # SILVER LAYER SCRIPT

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA ACCESS USING APP

# COMMAND ----------

# Azure Data Lake Storage authentication
# Configure credentials securely using Databricks Secret Scope.

storage_account = "<storage-account>"
tenant_id = dbutils.secrets.get(
    scope="<secret-scope>",
    key="<tenant-id-key>"
)
client_id = dbutils.secrets.get(
    scope="<secret-scope>",
    key="<client-id-key>"
)
client_secret = dbutils.secrets.get(
    scope="<secret-scope>",
    key="<client-secret-key>"
)

spark.conf.set(
    f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
    "OAuth"
)

spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
    client_id
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
    client_secret
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA LOADING

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Calender Data

# COMMAND ----------

# df_calendar = spark.read.format("csv") \
#         .option("header", "true") \
#         .option("inferSchema", "true") \
#         .load("abfss://bronze@adventureworkdatalake1.dfs.core.windows.net/AdventureWorks_Calendar")

# COMMAND ----------


# Bronze container path
base_path = "abfss://bronze@adventureworkdatalake1.dfs.core.windows.net/"

# List all folders in the Bronze container
items = dbutils.fs.ls(base_path)

folders = [item.path for item in items if item.isDir()]

print(f"Total folders found: {len(folders)}")
for folder in folders:
    print(folder)


# COMMAND ----------


# Dictionary to store each dataset separately
dataframes = {}

for folder_path in folders:
    folder_name = folder_path.rstrip("/").split("/")[-1]

    try:
        df = (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(folder_path)
        )

        dataframes[folder_name] = df

        print(f"Loaded: {folder_name}")

    except Exception as e:
            print(f"Could not load {folder_name}: {e}")

print(f"\nSuccessfully loaded {len(dataframes)} datasets.")


# COMMAND ----------


# Display all rows from every loaded dataset

# for name, df in dataframes.items():
#     print(f"Dataset: {name}")
#     print(f"Total rows: {df.count()}")
#     print(f"Total columns: {len(df.columns)}")
    
#     display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Calendar Transformation

# COMMAND ----------

display(dataframes["AdventureWorks_Calendar"])


# COMMAND ----------

# DBTITLE 1,Add Month to Calendar
AdventureWorks_Calendar = dataframes["AdventureWorks_Calendar"].withColumn("Month", month(col("Date")))\
                                                               .withColumn("Year", year(col("Date")))

# COMMAND ----------

AdventureWorks_Calendar.write.format("parquet") \
    .mode("append")\
        .option("path", "abfss://silver@adventureworkdatalake1.dfs.core.windows.net/AdventureWorks_Calendar")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer Transformation

# COMMAND ----------

display(dataframes["AdventureWorks_Customers"])


# COMMAND ----------

AdventureWorks_Customers = dataframes["AdventureWorks_Customers"].withColumn("fullname",concat(col("prefix"),lit(" "),col("FirstName"),lit(" "),col("LastName")))
AdventureWorks_Customers.display()

# COMMAND ----------

AdventureWorks_Customers_con = dataframes["AdventureWorks_Customers"].withColumn('fullname',concat_ws(' ',col('prefix'),col('FirstName'),col('LastName')))
AdventureWorks_Customers_con.display()

# COMMAND ----------

AdventureWorks_Customers_con.write.format("parquet") \
    .mode("append")\
        .option("path", "abfss://silver@adventureworkdatalake1.dfs.core.windows.net/AdventureWorks_Customers")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Categories Transformation

# COMMAND ----------

dataframes["AdventureWorks_Product_Categories"].display()

# COMMAND ----------

dataframes['AdventureWorks_Product_Categories'].write.format("parquet") \
    .mode("append")\
        .option("path", "abfss://silver@adventureworkdatalake1.dfs.core.windows.net/AdventureWorks_Product_Categories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sub Categories Transformation

# COMMAND ----------

display(dataframes["AdventureWorks_Product_Subcategories"])


# COMMAND ----------

dataframes["AdventureWorks_Product_Subcategories"].write.format("parquet") \
    .mode("append")\
        .option("path", "abfss://silver@adventureworkdatalake1.dfs.core.windows.net/AdventureWorks_Product_Subcategories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Products Transformation

# COMMAND ----------

dataframes["AdventureWorks_Products"].display()

# COMMAND ----------

AdventureWorks_Products = dataframes["AdventureWorks_Products"].withColumn("ProductSKU",split(col("ProductSKU"),'-')[0]) \
                                                                .withColumn("ProductName",split(col("ProductName"),' ')[0])
AdventureWorks_Products.display()

# COMMAND ----------

dataframes["AdventureWorks_Products"].write.format("parquet") \
    .mode("append")\
        .option("path", "abfss://silver@adventureworkdatalake1.dfs.core.windows.net/AdventureWorks_Products")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Return Transformation

# COMMAND ----------

dataframes["AdventureWorks_Returns"].display()

# COMMAND ----------

dataframes["AdventureWorks_Returns"].write.format("parquet") \
    .mode("append")\
        .option("path", "abfss://silver@adventureworkdatalake1.dfs.core.windows.net/AdventureWorks_Returns")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Territories Transformation

# COMMAND ----------

dataframes["AdventureWorks_Territories"].display()

# COMMAND ----------

dataframes["AdventureWorks_Territories"].write.format("parquet") \
    .mode("append")\
        .option("path", "abfss://silver@adventureworkdatalake1.dfs.core.windows.net/AdventureWorks_Territories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales Transformation

# COMMAND ----------

# AdventureWorks_Sales = reduce(DataFrame.unionAll, [
#     dataframes["AdventureWorks_Sales_2015"],
#     dataframes["AdventureWorks_Sales_2016"],
#     dataframes["AdventureWorks_Sales_2017"]
# ])
# AdventureWorks_Sales.display()

# COMMAND ----------

# dataframes["AdventureWorks_Sales_2015"].display()

# COMMAND ----------

df_allsales = spark.read.format("csv")\
                    .option("header", "true")\
                    .option("inferSchema", "true") \
                    .load("abfss://bronze@adventureworkdatalake1.dfs.core.windows.net/AdventureWorks_Sales*")


# COMMAND ----------

df_allsales.display()

# COMMAND ----------

df_allsales = df_allsales.withColumn('StockDate',to_timestamp('StockDate')) \
                         .withColumn('OrderNumber', regexp_replace('OrderNumber','S','T')) \
                         .withColumn('multiply',col('OrderLineItem')*col('OrderQuantity'))
df_allsales.display()

# COMMAND ----------

df_allsales.write.format("parquet") \
    .mode("append")\
        .option("path", "abfss://silver@adventureworkdatalake1.dfs.core.windows.net/AdventureWorks_Sales")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales Analysis and Visualization

# COMMAND ----------

df_allsales.groupBy('OrderDate').agg(count('OrderNumber').alias('total_order')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Categories Visualization

# COMMAND ----------

dataframes["AdventureWorks_Product_Categories"].display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Territory Analysis

# COMMAND ----------

dataframes["AdventureWorks_Territories"].display()