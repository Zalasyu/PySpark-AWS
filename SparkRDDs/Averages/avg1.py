from itertools import islice
from pyspark import SparkContext, SparkConf
import re

conf = SparkConf().setAppName("Average").setMaster("local[2]")
sc = SparkContext(conf=conf)

rdd = sc.textFile("movie_ratings.csv")

# Define a regular expression pattern to match commas inside double quotes
pattern = ',(?=(?:[^"]*"[^"]*")*[^"]*$)'


rdd = rdd.mapPartitionsWithIndex(
    lambda idx, it: islice(it, 1, None) if idx == 0 else it)
rdd = rdd.map(lambda x: re.split(pattern, x)).map(
    lambda x: (x[2], (float(x[3]), 1)))

rdd_reduceBy_year = rdd.reduceByKey(
    lambda x, y: (x[0] + y[0], x[1] + y[1]))            \
    .map(lambda x: (x[0], round(x[1][0]/x[1][1], 2)))   \
    .sortByKey(ascending=True)
print(rdd_reduceBy_year.collect())
