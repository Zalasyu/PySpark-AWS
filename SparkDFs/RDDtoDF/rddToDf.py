from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

studentSchema = StructType([
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("name", StringType(), True),
    StructField("course", StringType(), True),
    StructField("roll", StringType(), True),
    StructField("marks", IntegerType(), True),
    StructField("email", StringType(), True)
])

spark = SparkSession.builder.appName("rddToDf").getOrCreate()


rdd = spark.sparkContext.textFile("../Data/StudentData.csv")

header = rdd.first()

rdd = rdd.filter(lambda row: row != header).map(lambda row: row.split(","))

# Rectify type error when converting RDD to DataFrame
rdd = rdd.map(lambda row: [int(row[0]), row[1],
              row[2], row[3], row[4], int(row[5]), row[6]])

# Print how many partitions the RDD has
print(f"\n\nNumber of partitions: {rdd.getNumPartitions()}\n\n")

# Convert RDD to DataFrame
df = rdd.toDF(schema=studentSchema)

# Show the DataFrame
df.show()

# Print the schema
df.printSchema()
