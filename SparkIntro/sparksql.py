from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,StringType,StructType,StructField,DoubleType,FloatType
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("DataFrame-Basics") \
    .master('local[*]') \
    .getOrCreate()

# Before we worry about pulling in any data, let's just create some demo data
#With RDDs, our data was unstructured, but we had special methods available
# for key:value tuples. With DataFrames, we can just use normal tuples to create
# rows of data.
data = [ #empID, name, department, salary
    (1,'Alice','Engineering',80000.0),
    (2,'Bob','Marketing',6000.0),
    (3,'Charlie','Engineering',90000.0),
    (4,'Eve','Engineering',40000.0)
]

#Creating the dataframe in Spark, using the SparkSession object
#...looks a lot like pandas!
emp_df = spark.createDataFrame(data,['id','name','department','salary'])
emp_df.show()

# We can also create data frames with an explicit, type checked schema
# We construct a schema of StructType, which is a list of StructFields
# Each StructField contains name, type, isNullable (Frue or False)
emp_schema=StructType([
    StructField('id',IntegerType(),False),
    StructField('name',StringType(),True),
    StructField('department',StringType(),True),
    StructField('salary',DoubleType(),True)
])

emp_struct_df = spark.createDataFrame(data,emp_schema) # Creating a df with the schema
emp_struct_df.show()

#Just like Pandas, we can do things like select columns, add or modify columns...basically 1:1
emp_df.select('id','name').show()

# We can add a column
#Let's give everyone a 10% bonus with some really basic "feature eningeering"
bonus_df=emp_df.withColumn('bonus',col('salary')*0.10)
bonus_df.show()
# can work with it with pandas syntax
# emp_df.withColumn('bonus',emp_df['salary']*0.10).show()

#Pandas syntas for filtering off a column and its values
emp_df[emp_df['salary']>=50000.0].show()

#Also a "PySpark way" to do this with more functions
emp_df.filter(col('salary')>=50000.0).show()
################################### two ways to do it ^