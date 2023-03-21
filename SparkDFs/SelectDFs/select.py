from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("select").getOrCreate()

df = spark.read.csv("../Data/StudentData.csv",
                    header=True, inferSchema=True).cache()
