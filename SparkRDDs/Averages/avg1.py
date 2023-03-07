from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Partition").setMaster("local[2]")
sc = SparkContext(conf=conf)
