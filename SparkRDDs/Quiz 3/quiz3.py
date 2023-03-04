from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Quiz 3")

sc = SparkContext.getOrCreate(conf=conf)

rdd = sc.textFile("sample.txt")

rdd2 = rdd.flatMap(lambda x: x.split()).map(
    lambda x: (x, 1)).groupByKey().mapValues(len)

rdd3 = rdd.flatMap(lambda x: x.split()).map(
    lambda x: (x, 1)).reduceByKey(lambda x, y: x+y)

print(rdd2.collect())
print(rdd3.collect())
