from itertools import islice
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("mini-project").setMaster("local[2]")

sc = SparkContext(conf=conf)

rdd = sc.textFile("StudentData.csv")

rdd = rdd.mapPartitionsWithIndex(
    lambda idx, iter: islice(iter, 1, None) if idx == 0 else iter)

rdd = rdd.map(lambda x: x.split(","))

# Show the number of students in the file
rdd1 = rdd.count()
print(f"\n\n\nThere are {rdd1} students in the file\n\n\n")

# Show the total marks achieved by female and male students
rdd_male_count = rdd.filter(lambda x: x[1] == "Male").count()
rdd_female_count = rdd.filter(lambda x: x[1] == "Female").count()

print(
    f"\n\n\nThere are {rdd_male_count} male students and {rdd_female_count} female students.\n\n\n")

# Show the total number of students that have passed and failed.
#   50+ marks are required to pass the course.

rdd_passed = rdd.filter(lambda x: int(x[5]) > 50).count()
rdd_failed = rdd.filter(lambda x: int(x[5]) <= 50).count()

print(f"\n\n\nThere are {rdd_passed} students that have passed.\n\n\n")
print(f"\n\n\nThere are {rdd_failed} students that have passed.\n\n\n")

# Show the total number of students enrolled per course

rdd_per_course = rdd.map(lambda x: (x[3], 1)).reduceByKey(lambda x, y: x + y)

print(
    f"\n\n\nThe total number of students enrolled per course is: {rdd_per_course.collect()}\n\n\n")


# Show the total marks that students have achieved per course
rdd_total_marks = rdd.map(lambda x: (
    x[3], int(x[5]))).reduceByKey(lambda x, y: x + y)

print(
    f"\n\n\nThe total marks achieved per course is: {rdd_total_marks.collect()}\n\n\n")

# Show the average marks that students have achieved per course

rdd_avg_marks = rdd.map(lambda x: (
    x[3], (int(x[5]), 1))).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
rdd_avg_marks = rdd_avg_marks.map(lambda x: (x[0], x[1][0] / x[1][1]))
rdd6 = rdd_avg_marks.mapValues(lambda x: round(x, 2))

print(
    f"\n\n\nThe average marks achieved per course is: {rdd6.collect()}\n\n\n")

# Show the minimum and maximum marks achieved per course
rdd_max = rdd.map(lambda x: (x[3], int(x[5]))).reduceByKey(
    lambda x, y: x if x > y else y)
rdd_min = rdd.map(lambda x: (x[3], int(x[5]))).reduceByKey(
    lambda x, y: x if x < y else y)

print(
    f"\n\n\nThe maximum marks achieved per course is: {rdd_max.collect()}\n\n\n")
print(
    f"\n\n\nThe minimum marks achieved per course is: {rdd_min.collect()}\n\n\n")

# Show the average age of male and female students

rdd_avg_age_per_sex = rdd.map(lambda x: (x[1], (int(x[0]), 1))).reduceByKey(
    lambda x, y: x + y).map(lambda x: (x[0], x[1][0] / x[1][1]))

print(
    f"\n\n\nThe average of male and females are {rdd_avg_age_per_sex.collect()}\n\n\n")
