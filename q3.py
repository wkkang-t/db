import sys
from pyspark.sql import SparkSession
import ast
# you may add more import if you need to


# don't change this line

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW

rdd1 = spark.read.option("header", True).option("inferSchema", "true")\
.csv("data/TA_restaurants_curated_cleaned.csv").rdd
def extract_review_date_pair(line):
    ls = ast.literal_eval(line[8]) # convert string to list
    reviews = ls[0]
    dates = ls[1]
    review_date = []
    for review, date in zip(reviews, dates):
        review_date.append((line[10], review, date))
    return review_date
  
rdd1 = rdd1.flatMap(extract_review_date_pair)
df = spark.createDataFrame(rdd1).toDF("ID_TA", "review", "date")
df.write.option("header", True)\
.csv("q3")
