from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Quiz 2")

sc = SparkContext.getOrCreate(conf=conf)

rdd = sc.textFile("sample.txt")


def filter_func(word):
    if word.startswith("a") or word.startswith("c"):
        return False
    else:
        return True


rdd2 = rdd.flatMap(lambda x: x.split(" ")).filter(
    lambda x: not (x.startswith("a") or x.startswith("c")))

print(rdd2.collect())
