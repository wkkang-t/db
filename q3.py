import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import arrays_zip, explode, split

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW
df = spark.read.option("header",True)\
    .option("inferSchema", True)\
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

#replace Reviews w a list in the form of [{review a, date a}, {review b, date b}]
df2 = df.withColumn("Reviews", arrays_zip(*"Reviews"))

#explode to split the list into rows
df2 = df2.select(df2.ID_TA, explode(df2.Reviews))

#now we should have id_ta in one column and second column is Reviews that is a list in the form [review, date]
df2 = df2.select(df2.ID_TA, (df2.Reviews[0]).alias("review"), (df2.Reviews[1]).alias("date"))

df2.show()

df2.write.csv("hdfs://%s:9000/assignment2/output/question3/" % (hdfs_nn), header=True)