from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("select").getOrCreate()

df = spark.read.csv("../Data/StudentData.csv",
                    header=True, inferSchema=True).cache()

# Create view for SQL queries
df.createOrReplaceTempView("df")

# Show the first 5 rows
df.show(5)

# Select the name and email columns (DataFrame API Select Query)
df.select("name", "email").show(5)

# Select the name and email columns (DataFrame API Select Query)
df.select(df.name, df.email).show(5)

# Select the name, age, and marks (SQL Select Query)
spark.sql("SELECT name, age, marks FROM df").show(5)

#
df.select(col("name"), col("age"), col("gender")).show(5)

# Stop the SparkSession
spark.stop()
