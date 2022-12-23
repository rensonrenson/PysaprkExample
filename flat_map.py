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
    # spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()
    #
    # arrayData = [
    #     ('James', ['Java', 'Scala'], {'hair': 'black', 'eye': 'brown'}),
    #     ('Michael', ['Spark', 'Java', None], {'hair': 'brown', 'eye': None}),
    #     ('Robert', ['CSharp', ''], {'hair': 'red', 'eye': ''}),
    #     ('Washington', None, None),
    #     ('Jefferson', ['1', '2'], {})]
    # df = spark.createDataFrame(data=arrayData, schema=['name', 'knownLanguages', 'properties'])
    # df2 = df.select(df.name, explode(df.knownLanguages))
    # df2.printSchema()
    # df2.show()
