import sys
from pyspark.sql import SparkSession

# you may add more import if you need to


# don't change this line

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()

input = spark.read.option("header", True).option("inferSchema", True)\
.csv("data/TA_restaurants_curated_cleaned.csv")

inputRdd = input.filter(input["Price Range"].isNotNull()).filter(
input["Rating"].isNotNull()).rdd

inputRdd = inputRdd.map(lambda line : (line[2] + line[6], line) ).cache()

worst = inputRdd.reduceByKey(lambda x, y: x if x[5] < y[5] else y)
        
best = inputRdd.reduceByKey(lambda x, y: x if x[5] > y[5] else y)

results = worst.union(best).sortByKey().map(lambda line : line[1])

dfWithSchema = spark.createDataFrame(results).toDF("_c0", "Name", "City", "Cuisine" "Style" , "Ranking", "Rating", "Price Range", "Number of Reviews", "Reviews", "URL_TA", "ID_TA")

dfWithSchema.write.csv("q2out.csv")
