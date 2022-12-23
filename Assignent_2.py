from datetime import datetime

import findspark


from explode import spark

findspark.init()
from pyspark.sql.functions import *

from pyspark.sql.functions import col

# Read the table in csv file to create dataframe

df = spark.read.option("header",True).option("inferSchema",True).csv("resources/assigment.csv")
df1 = spark.read.option("header",True).option("inferSchema",True).csv("resources/assingment.csv")

# convert time stamp value to calculate millisecond

df1.withColumn("timestamp", to_timestamp(col("StartTime"))).withColumn("millisecond", unix_timestamp(col("timestamp"))).show(truncate=False)

# join dataframe for two table use Product_Number
df.join(df1,df["Product_Number"] ==  df1["Product_Number1"],"outer").show(truncate=False)

# show only Country =EN

df.join(df1,df["Product_Number"] ==  df1["Product_Number1"],"outer").filter("Language == 'EN'").show(truncate=False)