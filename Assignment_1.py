import findspark
findspark.init()
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when

spark = SparkSession.builder.appName('Assignment one').getOrCreate()

#Read csv file for data to convert a dataframe

df = spark.read.option("header",True).option("inferSchema",True).csv("resources/assigment.csv")

# convert millisecound to time stamp

df1 = df.withColumn("equal_time", to_timestamp(col("Issue_Date") / 1000)).show(truncate=False)
# convert time stamp value to date format

df2 = df.withColumn("equal_time",F.from_unixtime(F.col("Issue_Date") / 1000)).withColumn("epoch_milli",F.concat(F.unix_timestamp("equal_time"), F.date_format("equal_time", "S")))
df2.show(truncate=False)

# Remove the null value to given space
df.withColumn("Country",when(col("Country")=="null" ,"").otherwise(col("Country"))).show()

# Remove white space before the value
df.withColumn("col2",ltrim("Brand")).show()