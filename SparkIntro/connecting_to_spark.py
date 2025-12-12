# We want to make sure that we can connect to spark from our code
# We need to import some stuff
from pyspark.sql import SparkSession
import sys
# So far we've been using SparkContext - here we will use a SparkSession
# SparkSession is a unified entry point that was created with Spark 2.0

print("Hello World!")

#Creating my spark session
#MasterURL - points to a remote cluster or your local machine
#Using slashes to avoid long single lines
spark = SparkSession.builder.appName("SparkConnectionDemo")\
    .master("local[*]")\
    .getOrCreate()

#Now that we have our session, we can ask for the SparkConxtext within it
#If we just wanna do RDD stuff

#We don't create a new context, we ask our SparkSession object for its context
sc=spark.sparkContext
data=[1,2,3,4,5,6,7]
test_rdd=sc.parallelize(data) #Turn dummy data into an RDD

#Basic transformations (lazy)
squared_rdd=test_rdd.map(lambda x: x**2) # Squaring numbers
evens_rdd=squared_rdd.filter(lambda x:x%2==0) # Filtering for evens
#Action triggers actual computation
result=evens_rdd.collect()
print(result)

