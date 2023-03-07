from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Count By Value Practice")
sc = SparkContext.getOrCreate(conf=conf)

rdd = sc.textFile("numbers.txt")

rdd2 = rdd.map(lambda x: int(x)).countByValue()

filter_dict = {k: v for k, v in rdd2.items() if v > 1}

print(filter_dict)
