from datetime import datetime

import findspark
from pandas.io.html import _remove_whitespace

findspark.init()
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when

spark = SparkSession.builder \
          .appName('Date Convert') \
          .getOrCreate()

df = spark.read.option("header",True).option("inferSchema",True).csv("resources/assigment.csv")

df1 = df.withColumn("equal_time", to_timestamp(col("Issue_Date") / 1000)).show(truncate=False)



df2 = df.withColumn("equal_time",F.from_unixtime(F.col("Issue_Date") / 1000)).withColumn("epoch_milli",F.concat(F.unix_timestamp("equal_time"), F.date_format("equal_time", "S")))
df2.show(truncate=False)

df.withColumn("Country",when(col("Country")=="null" ,"").otherwise(col("Country"))).show()



