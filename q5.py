import sys 
from pyspark.sql import SparkSession
import ast
# you may add more import if you need to

# don't change this line

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW


input = spark.read.option("header", True).option("inferSchema", "true")\
.parquet("data/tmdb_5000_credits.parquet")

inputRdd = input.rdd

# create new rdd with co-cast actors
# create new rdd with co-cast actors
# create new rdd with co-cast actors
def extract_actors(line):
    cast_ls = ast.literal_eval(line[2])
    answer = []
    actors = []
    for i in range(len(cast_ls)-1):
        for j in range(i+1,len(cast_ls)):
            if cast_ls[i]["name"] > cast_ls[j]["name"]:
                actor = cast_ls[i]["name"] + ',' + cast_ls[j]["name"]
            else:
                actor = cast_ls[j]["name"] + ',' + cast_ls[i]["name"]
            if actor not in actors:
                actors.append(actor)
                answer.append((str(line[0]) +','+line[1], actor) )
    return answer

movieWithActors = inputRdd.flatMap(extract_actors)

actorsCrossActors =movieWithActors.map(lambda line: (line[1], line[0]))

counts = actorsCrossActors.map(lambda line: (line[0], 1)).reduceByKey(lambda x,y: x+y).filter(lambda line: line[1] >= 2).toDF()

# 
# actorsCrossActressMore2 = actorsCrossActors.join(counts).sortByKey().map(lambda line: (line[1][0].split(',')[0], line[1][0].split(',')[1], line[0].split(',')[0], line[0].split(',')[1])).distinct()
# 
# dfWithSchema = spark.createDataFrame(actorsCrossActressMore2).toDF("movie_id", "title", "actor1", "actor2")

counts.write.option("header", True).csv("q5_4")