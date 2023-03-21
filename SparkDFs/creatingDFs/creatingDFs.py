from pyspark.sql import SparkSession

# Create a SparkSession
sc = SparkSession.builder.appName("CreatingDFs").getOrCreate()

# Create a DataFrame from a csv file
df = sc.read.csv("../Data/StudentData.csv", header=True).cache()

# Show the DataFrame
df.show()

# Stop the SparkSession
sc.stop()
