from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("name", StringType(), True),
    StructField("course", StringType(), True),
    StructField("roll", StringType(), True),
    StructField("marks", IntegerType(), True),
    StructField("email", StringType(), True),
])


spark = SparkSession.builder.appName("provideSchema").getOrCreate()

df = spark.read.csv("../Data/StudentData.csv",
                    header=True, schema=schema).cache()

df.show()

df.printSchema()

spark.stop()
