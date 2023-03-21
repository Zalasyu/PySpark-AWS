from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SchemaDFs").getOrCreate()

df = spark.read.csv("../Data/StudentData.csv",
                    header=True, inferSchema=True).cache()

# Show the DataFrame
df.show()

# Print the schema
df.printSchema()

# Stop the SparkSession
spark.stop()
