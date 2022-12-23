import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = ["Hello to all",
        "Youe are welcome to Diggibyte ",
        "Hope very one doing well",
        "Be safe with happy to live",
        "Thank you to all"]
rdd=spark.sparkContext.parallelize(data)
for element in rdd.collect():
    print(element)

#Flatmap
rdd2=rdd.flatMap(lambda x: x.split(" "))
for element in rdd2.collect():
    print(element)

