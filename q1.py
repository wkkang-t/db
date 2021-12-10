import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# you may add more import if you need to


spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()


input = spark.read.option("header", True).option("inferSchema", "true")\
.csv("./data/TA_restaurants_curated_cleaned.csv")

cleanedData = input.filter(input["Reviews"] != "[ [  ], [  ] ]").filter( input["Rating"] >= 1.0)
cleanedData.write.csv("./q1")
