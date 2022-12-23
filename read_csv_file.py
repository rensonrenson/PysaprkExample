import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType
spark = SparkSession.builder.master("local").appName("hello spark").getOrCreate();
df = spark.read.csv("resources/Customer.csv")
df1 = spark.read.json("resources/sample.json")
df2 = spark.read.parquet("resources/userdata.parquet")
# df3 = spark.read.avro("resources/example.avro")
df.printSchema()
df.show()
df1.show()
df2.show()

