import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, desc, length, asc, lit, explode

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header",True)\
    .option("inferSchema", True)\
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

# add column "Cuisine" that is just a list of the cuisines
# since we start with a string, we first remove all the additional symbols, then split by ","
df2 = df.withColumn("Cuisine", split(regexp_replace("Cuisine Style", r"(^\[)|(\]$)", ""), ", "))

# select name, city and creates a new row for each cuisine in the array (explode)
df2 = df2.select(df2.City, explode(df2.Cuisine))

#group by the city and cuisine + add the count
df2 = df2.groupBy(["City", "Cuisine"]).agg(count("Cuisine").alias("Count").sort("City")

df2.show()

df2.write.csv("hdfs://%s:9000/assignment2/output/question4/" % (hdfs_nn), header=True)

