from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("MaxMin").setMaster("local[2]")

sc = SparkContext(conf=conf)

rdd = sc.textFile("average_quiz_sample.csv")

rdd = rdd.map(lambda x: x.split(",")).map(lambda x: (x[1], float(x[2])))

print(rdd.reduceByKey(lambda x, y: x if x > y else y).collect())
print(rdd.reduceByKey(lambda x, y: x if x < y else y).collect())
