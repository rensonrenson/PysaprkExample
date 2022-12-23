from datetime import datetime

import findspark
from pandas.io.html import _remove_whitespace

findspark.init()
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when

import re



spark = SparkSession.builder \
          .appName('SparkByExamples.com') \
          .getOrCreate()

df = spark.read.option("header",True).option("inferSchema",True).csv("resources/assigment.csv")

df1 = df.withColumn("equal_time", to_timestamp(col("Issue_Date") / 1000)).show(truncate=False)



df2 = df.withColumn("equal_time",F.from_unixtime(F.col("Issue_Date") / 1000)).withColumn("epoch_milli",F.concat(F.unix_timestamp("equal_time"), F.date_format("equal_time", "S")))
df2.show(truncate=False)



df.withColumn("Country", \
       when(col("Country")=="null" ,"") \
          .otherwise(col("Country"))) \
  .show()






# df.columns = (df.columns.str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True).str.upper())


#df.select(rtrim(col("Brand"))).show()
# df.withColumn("col2",trim("Brand")).show()
# df.withColumn("col2",ltrim("Brand")).show()
# df.withColumn("col2",rtrim("Brand")).show()
#
# df = spark.sql("select '1636663343887' as epoch_ms")
# df.withColumn(
#     "eq_time",
#     F.to_timestamp(F.col("epoch_ms") / 1000)
# )
df1 = spark.read.option("header",True).option("inferSchema",True).csv("resources/assingment.csv")
#
# df2 =df1.withColumn("epoch_milli",F.concat(F.unix_timestamp("StartTime"), F.date_format("StartTime", "D")))
# df2.show(truncate=False)
# # df2 = df1.withColumn("epoch_milli",F.concat(F.unix_timestamp("StartTime"), F.date_format("eq_time", "S")))
# # df2.show(truncate=False)


# from pyspark.sql.functions import col
#
# # df = spark.sql("select '1636663343887' as epoch_seconds")
# # df = df.withColumn("eq_time", (col("epoch_seconds") / 1000).cast("timestamp"))
# df1 = df1.withColumn("epoch_sec", (col("StartTime").cast("double")))
#
df1.show()
# df2 = df1.withColumn("timestampUnixTimestamp", unix_timestamp(col("StartTime"))).show()
df1.withColumn("timestamp", to_timestamp(col("StartTime"))).withColumn("millisecond", unix_timestamp(col("timestamp"))).show(truncate=False)

df.join(df1,df["Product_Number"] ==  df1["Product_Number1"],"outer").show(truncate=False)

df.join(df1,df["Product_Number"] ==  df1["Product_Number1"],"outer").filter("Language == 'EN'").show(truncate=False)
