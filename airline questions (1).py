# Databricks notebook source
#Find the difference of the prices on each day with its previous day.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag



# COMMAND ----------

spark = SparkSession.builder.appName("impetus").getOrCreate()
data = [(1,'Indigo',7000,'2023-01-01'),
        (1,'Indigo',7500,'2023-01-05'),
        (1,'Indigo',7100,'2023-02-10'),
        (1,'Indigo',8200,'2023-02-15'),
        (1,'Indigo',8500,'2023-03-04'),
        (2,'Vistara',8000,'2023-01-02'),
        (2,'Vistara',8500,'2023-01-06'),
        (2,'Vistara',8200,'2023-02-12'),
        (2,'Vistara',9200,'2023-02-18'),
        (2,'Vistara',9500,'2023-03-06')]
column = ['flight_id','name','price','price_date']
df = spark.createDataFrame(data,column)
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Partitioning devides the data into smaller chunksbased on column values

# COMMAND ----------

#we partioned the airline id so each airline's data is calculated independently and order by date:
windowSpec = Window.partitionBy("flight_id").orderBy("price_date")


# COMMAND ----------

# MAGIC %md
# MAGIC The lag() function is a window function used to access data from a previous row in the same result set without using self-joins.

# COMMAND ----------

# Use lag() to Get Previous Price
dfwithPrev = df.withColumn("Prev price", lag("price", 1).over(windowSpec))
dfwithPrev.show()

# COMMAND ----------

#Calculate Price Difference
priceDiff = dfwithPrev.withColumn("Price_diff", col("price")- col("Prev price"))
priceDiff.show()

# COMMAND ----------

priceDiff.orderBy("flight_id", "price_date").show()