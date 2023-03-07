from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Save Text File Practice").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf=conf)

rdd = sc.parallelize(["Hello", "World", "This", "is", "a", "test", "file"])
rdd.saveAsTextFile("output.txt")
