import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("hello spark").getOrCreate();

df = spark.read.csv("resources/Customer.csv").show()
df1 = spark.read.json("resources/sample.json").show()
df2 = spark.read.parquet("resources/userdata.parquet").show()
