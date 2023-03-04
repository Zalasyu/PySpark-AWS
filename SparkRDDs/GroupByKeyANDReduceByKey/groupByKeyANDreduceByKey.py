# Databricks notebook source
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("GroupByKeyANDReduceByKey")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('sample.txt')

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x: x.split())

# COMMAND ----------

rdd3 = rdd2.map(lambda x: (x, len(x)))

# COMMAND ----------

print(rdd3.groupByKey().mapValues(list).collect())

# COMMAND ----------

print(rdd3.reduceByKey(lambda x, y: x+y).collect())

# COMMAND ----------
