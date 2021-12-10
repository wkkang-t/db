import sys
from pyspark.sql import SparkSession
import ast
# you may add more import if you need to


# don't change this line

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW

input = spark.read.option("header", True).option("inferSchema", "true")\
.csv("data/TA_restaurants_curated_cleaned.csv") #dataframe

inputRdd = input.filter(input["City"].isNotNull()).filter(input["Cuisine Style"].isNotNull()).rdd

def extract_city_style_pair(line):
    style_ls = ast.literal_eval(line[3]) # convert string to list
    answer = []
    for style in style_ls:
        answer.append( ( line[2] + ','+ style  , 1) )
    return answer
  
inputRdd = inputRdd.flatMap(extract_city_style_pair)
        
counts = inputRdd.reduceByKey(lambda x, y: x + y).map(lambda line:( line[0].split(',')[0], line[0].split(',')[1], line[1]) )

dfWithSchema = spark.createDataFrame(counts).toDF("City", "Cuisine", "count")

dfWithSchema.write.csv("q4")
