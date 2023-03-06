import random
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Filter Practice").setMaster(
    "local[*]").set("spark.driver.cores", "2").set("spark.driver.memory", "2g")

sc = SparkContext.getOrCreate(conf=conf)

# Create a textfile with random numbers
with open("numbers.txt", "w") as f:
    for i in range(1000):
        f.write(str(random.randint(0, 10000)) + "\n")


def is_odd(x):
    return int(x) % 2 == 1


rdd = sc.textFile("numbers.txt")

# Filter the RDD to only contain numbers divisible by 2
rdd2 = rdd.filter(lambda x: int(x) % 2 == 0)

rdd3 = rdd2.distinct()

# Print the first 10 numbers in the RDD
print(rdd3.collect())
