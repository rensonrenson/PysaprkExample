import findspark
findspark.init()
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from explode import spark

#create a schema for csv files

schema = StructType([
    StructField("EmployeeID", IntegerType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("Address", StringType(), True),

])
# the schema apply for csv files

df = spark.read.schema(schema).csv("resources/emp_details.csv")
df.printSchema()

# df.filter(df("EmployeeName") == "arul").show(False)
df.filter("EmployeeID == '1001'").show()
df.filter("EmployeeName == 'arun'").show()
