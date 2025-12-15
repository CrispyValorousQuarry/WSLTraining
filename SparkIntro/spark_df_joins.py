from pyspark.sql import SparkSession
from pyspark.sql.functions import col

#Create a Spark Session
spark = SparkSession.builder \
    .appName('Dataframe Joins in Spark') \
    .master('local[*]') \
    .getOrCreate()

# Let's create 2 related dataframes as if we were
# pulling data from a SQL database

#Two tables - employees and department
#Each employee has 1 department
#Each department has (potentially) multiple employees (1->M)
employees = spark.createDataFrame([
    (1,'Alice',101), #empID, name, deptID
    (2,'Bobby',102),
    (3,'Chris',102),
    (4,'Dana', None) #No dept foreign key
], ['emp_id','emp_name','dept_id']) # createDataFrame(data,[columns])

departments = spark.createDataFrame([
    (101, 'Engineering','Building A'), #deptID, name, location
    (102, 'Marketing', 'Building B')
], ['dept_id', 'dept_name','location'])

employees.show()
departments.show()

# SparkSQL provides for SQL like querying without having to use actual SQL
# This can make working with multiple data frames really easy, no need to
# set up data pipelines the RDD way where you cahin function calls together

# It supports all the SQL joins we're used to

# Inner join
inner_result=employees.join(
    departments, #The df we're joining
    employees.dept_id==departments.dept_id, #column joining on
    'inner' #what kind of join? Inner, left, right, etc
).select(employees['*']) #Selecting everything from employees who have a match

alt_inner_result=employees.join(departments,'dept_id') \
    .select(employees['*'],departments.dept_name)

alt_inner_result.show()
inner_result.show()