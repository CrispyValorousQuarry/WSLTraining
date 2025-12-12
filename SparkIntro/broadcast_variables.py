from pyspark import SparkContext
#pointing to our local manager
sc=SparkContext("local[*]","BroadcastDemo")

country_lookup={
    "US": 'United States',
    'UK': 'United Kingdom',
    'DE': 'Germany',
    'FR': 'France',
    'JP': 'Japan'
}
# First we'll do this without a broadcast variable
# Some country codes in an RDD (abbreviated codes)
data_rdd=sc.parallelize(['US','UK','DE','US','FR','JP','MN'])

#Creating a method to look up country codes - no broadcast
def lookup_country_bad(code):
    #Dictionary lookup - straight python; if no value then passes 'unknown'
    return country_lookup.get(code,'Unknown')
    #For every record we run thought in this RDD
    #The node(s) have to have a copy of the lookup dictionary
    #sent to them over the network. EVERY. SINGLE. TIME.
    #If the data rdd is 100,000 items long, that's 100k
    #transfers of the same lookup dictionary for no reason.

#With a broadcast variable
#Using a broadcast variable we let the Driver ndoe know to send this data
#to every executor once. Then instruct the executor to cache it locally
#until it's done with the transformation

#Let's create a broadcast variable
bc_country_lookup=sc.broadcast(country_lookup)

def country_lookup_good(code):
    return bc_country_lookup.value.get(code,'Unknown') # Using our broadcast variable

results=data_rdd.map(country_lookup_good).collect()
#We want to get into the habit of cleaning up our broadcast variables whenever we use them
#otherwise the executors will keep these cached on the worker's node memory
bc_country_lookup.unpersist() #tells executors to dereference this variable to free worker node memory
print(results)