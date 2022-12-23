from datetime import datetime

import findspark
from explode import spark
findspark.init()
import pandas as pd
from pandas.compat import StringIO
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql import functions as F
from pyspark.sql import types as T



df = spark.createDataFrame(
    data=[
        (1001, "arun",  datetime.strptime("1999-12-19", "%Y-%m-%d").date()),
        (1002, "arul",  datetime.strptime("1989-12-14", "%Y-%m-%d").date()),
    ],
    schema=T.StructType([
        T.StructField("id", T.IntegerType(), True),
        T.StructField("name", T.StringType(), True),
        T.StructField("birth_date", T.DateType(), True)
    ])
)
df = df.withColumn("age_date", F.floor(F.datediff(F.current_date(), F.col("birth_date")) / 365.25))
df.show()
