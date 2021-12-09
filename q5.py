import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, udf
import re
from itertools import combinations
from pyspark.sql.types import StringType
# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW
#f = spark.read.option("header",True)\
#.parquet("hdfs://%s:9000/assignment2/part2/input/" % (hdfs_nn))
#df.printSchema()
#df.show()

parquetFile = spark.read.option("header",True).parquet("hdfs://%s:9000/assignment2/part2/input/" % (hdfs_nn))
#cast = parquetFile.collect()

def getUniquePairs(cast):
    uniquePairs = []
    for row in cast:
       # castList = row["cast"]
        list = re.split('": |, |" | ' ' |"',cast)
        list = [i for i in list if i]
        names = []
        i = 1
        for j in list:
            if j == "name":
                names.append(list[i])
            i +=1
            uniquePairs.append([",".join(map(str, comb)) for comb in combinations(names, 2)])
    return uniquePairs

udfGetUniquePairs = udf(lambda z:getUniquePairs(z), StringType())

df = parquetFile.withColumn("unique_Pairs", udfGetUniquePairs("cast"))
dfOut = df.select("movie_id", "title", "unique_pairs")
#dfOut.show()
#parquetFile.withColumn('unique_pairs', uniquePairs).collect()

#df.write.csv("hdfs://%s:9000/assignment2/output/question5/" % (hdfs_nn), header=True) 
dfOut.write.parquet("hdfs://%s:9000/assignment2/output/question5/" % (hdfs_nn))

