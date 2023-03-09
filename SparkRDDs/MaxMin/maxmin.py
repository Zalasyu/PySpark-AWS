
from itertools import islice
from pyspark import SparkContext, SparkConf
import re

conf = SparkConf().setAppName("MaxMin").setMaster("local[2]")
sc = SparkContext(conf=conf)

rdd = sc.textFile("movie_ratings.csv")

# Define a regular expression pattern to match commas inside double quotes
pattern = ',(?=(?:[^"]*"[^"]*")*[^"]*$)'


rdd = rdd.mapPartitionsWithIndex(
    lambda idx, it: islice(it, 1, None) if idx == 0 else it)
rdd2 = rdd.map(lambda x: re.split(pattern, x)).map(
    lambda x: (x[2], (x[1], float(x[3]))))
rdd3 = rdd.map(lambda x: re.split(pattern, x)).map(
    lambda x: (x[2], float(x[3])))

rdd_max3 = rdd3.reduceByKey(lambda x, y: x if x > y else y)
rdd_min3 = rdd3.reduceByKey(lambda x, y: x if x < y else y)
rdd_max2 = rdd2.reduceByKey(lambda x, y: x if x[1] > y[1] else y)
rdd_min2 = rdd2.reduceByKey(lambda x, y: x if x[1] < y[1] else y)

# With movie title in record
print(rdd_max2.collect())
# Without movie title in record
print(rdd_max3.collect())

# With movie title in record
print(rdd_min2.collect())
# Without movie title in record
print(rdd_min3.collect())
