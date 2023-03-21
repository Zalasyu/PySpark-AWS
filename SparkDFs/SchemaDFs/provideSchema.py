from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("provideSchema").getOrCreate()

df = spark.read.csv("../Data/StudentData.csv",
                    header=True, inferSchema=True).cache()

df.show()

df.printSchema()

spark.stop()
