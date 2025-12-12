from pyspark import AccumulatorParam, SparkContext

sc = SparkContext('local[*]','AccumulatorsDemo')
#Let's create some accumulators
#We will have some intentionally bad data, and we want a count
#of both total records processed, and how many of those are invalid

#They are created like other python pyspark objects
total_records=sc.accumulator(0) #start at initial/default value
error_count=sc.accumulator(0) 

#Sample data with bad records
data=[
    'valid',
    'valid',
    'bad',
    'invalid',
    'valid'
]

rdd=sc.parallelize(data) #Turn data into an rdd

#We want to be able to give a map some more custom logic
#All a map does is apply a function to everything in a collection (rdd)
#since this function will be passed into a map and map runs on worker nodes
# (inside an executor), we can't create accumulators within process_record
#When working in PySpark we need to be mindful not just of python scoping
#but driver vs worker scoping
def process_record(record):
    #Whether a record is good or bad, we tried. So we increment total_records
    total_records.add(1)
    if(record!='valid'):
        #If record isn't valid we increment error_count
        error_count.add(1)
        #If the record isn't valid, we simply return None
        return None
    return record #If record is valid we never hit the if-block

#Now that we wrote our filtering function we can use it in a transformation
#When doing transformations, we can chain them
#Each transformation in the chain runs sequentially.
#We run process_record() for every record in our initial RDD
# rdd=['valid','valid','bad','invalid','valid']
# We run map(process_record)...we get
# ['valid','valid',None,None,'valid']
# then we run filter(lambda x:x is not None) on the above
# inside of results...we get ['valid','valid','valid']
results=rdd.map(process_record).filter(lambda x: x is not None)

#Now we can finally trigger an action
valid_values=results.collect()
print(valid_values)
print(total_records.value)
print(error_count)

###################################################################
#Custom accumulator example

#To use my custom accumulator, it's a bit like creating a custom exception
class myStringAccumulator(AccumulatorParam): #I need to implement a few methods
    #reset - resetting the accumulator (optional)
    #add - logic to add a value from a worker task to the accumulator
    #zero - what is the logic to determine the default value
    def zero(self, initial_value):
        return set() # return an empty python set
    
    def addInPlace(self, value1, value2):
        return value1.union(value2)
#create with initial value, what kind of accumulator
unique_words=sc.accumulator(set(),myStringAccumulator)

def collect_words(word):
    unique_words.add(word)