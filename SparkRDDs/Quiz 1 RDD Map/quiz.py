from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Quiz 1")

sc = SparkContext.getOrCreate(conf=conf)

rdd = sc.textFile("sample.txt")

rdd2 = rdd.map(lambda x: list(map(len, x.split())))

print("The number of words in each line:")
print(rdd2.collect())

rdd3 = rdd2.flatMap(lambda x: x)
print("The number of words in the file flattened: ")
print(rdd3.collect())
