import sys
from pyspark.sql import SparkSession
import ast
# you may add more import if you need to


# don't change this line

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW

inputRdd = spark.read.option("header", True).option("inferSchema", "true")\
.csv("data/TA_restaurants_curated_cleaned.csv").rdd

def extract_review_date_pair(line):
    ls = ast.literal_eval(line[8]) # convert string to list
    reviews_ls = ls[0]
    dates_ls = ls[1]
    answer = []
    for review, date in zip(reviews_ls, dates_ls):
        answer.append((line[10], review, date))
    return answer
  
results = inputRdd.flatMap(extract_review_date_pair)

dfWithSchema = spark.createDataFrame(results).toDF("ID_TA", "review", "date")

dfWithSchema.write.option("header", True)\
.csv("q3")
