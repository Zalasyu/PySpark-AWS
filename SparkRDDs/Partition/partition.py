from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Partition").setMaster("local[*]")
sc = SparkContext(conf=conf)

rdd = sc.textFile("numbers.txt")
print("=====================================")
print(f"The number of partitions in rdd2 is {rdd.getNumPartitions()}")
print(f"The average size of each partition is{rdd.glom().map(len).mean()}")
print("=====================================")
rdd = rdd.repartition(5)
rdd2 = rdd.map(lambda x: (x, 1))
# 5 partitions
print("=====================================")
print(f"The number of partitions in rdd2 is {rdd2.getNumPartitions()}")
print(f"The average size of each partition is {rdd2.glom().map(len).mean()}")
print("=====================================")
print(rdd2.take(10))
print("=====================================")
