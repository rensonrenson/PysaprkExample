import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import explode_outer

spark = SparkSession.builder.appName('Explode - Examples').getOrCreate()


arrayData = [('James',['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Robert',['CSharp',''],{'hair':'red','eye':''}),
        ('Washington',None,None),
        ('Jefferson',['1','2'],{})]
print(arrayData)
df=spark.createDataFrame(data=arrayData,schema=["name","knownLanguages","properties"])
df.printSchema()
df.show()
#explode for array

df2 = df.select(df.name,explode(df.knownLanguages))
df2.printSchema()
df2.show()

#explode for map

from pyspark.sql.functions import explode
df3 = df.select(df.name,explode(df.properties))
df3.printSchema()
df3.show()

#explode for expolde outer
df.select(df.name,explode_outer(df.knownLanguages)).show()
