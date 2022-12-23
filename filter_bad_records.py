import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.master("local").appName("Filter bad records").getOrCreate();

from pyspark.sql.types import StructType, StructField, IntegerType, StringType



schema = StructType([
    StructField("EmployeeID", IntegerType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("corrupt_record", StringType(), True)
])
# the schema apply for csv files

df = spark.read.option("mode", "PERMISSIVE").schema(schema).option("header", True).option("columnNameOfCorruptRecord", "_corrupt_record").csv("resources/emp_details.csv").cache()
df.printSchema()

# drop the corrupt record in the csv files

df.where(col("corrupt record").isNull()).drop("corrupt_record")
df.show()

# find which records are corrupt in files

df.select(col("corrupt record")).where(col("corrupt_record").isNotNull())
df.show()