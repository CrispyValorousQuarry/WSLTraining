from pyspark import SparkContext

sc = SparkContext('local[*]','Transformations')

# Sample log data
logs = sc.parallelize([
    "2024-01-15 10:00:00 INFO User login: alice",
    "2024-01-15 10:01:00 ERROR Database connection failed",
    "2024-01-15 10:02:00 INFO User login: bob",
    "2024-01-15 10:03:00 WARN Memory usage high",
    "2024-01-15 10:04:00 ERROR Timeout occurred",
    "2024-01-15 10:05:00 INFO Data processed: 1000 records",
    "2024-01-15 10:06:00 DEBUG Cache hit rate: 95%"
])

# Task A: Filter only ERROR logs
errors = logs.filter(lambda x: 'ERROR' in x)
print(f"Errors: {errors.collect()}")

# Task B: Extract just the log level from each line
# Expected: ["INFO", "ERROR", "INFO", "WARN", "ERROR", "INFO", "DEBUG"]
levels = logs.map(lambda x: x.split(' ')[2])
# YOUR CODE (hint: split and take index 2)
print(f"Levels: {levels.collect()}")

# Task C: Chain - get messages from ERROR logs only
# Expected: ["Database connection failed", "Timeout occurred"]
error_messages = logs.filter(lambda x:'ERROR' in x)\
.map(lambda x: ' '.join(x.split(' ')[3::1]))
print(f"Error messages: {error_messages.collect()}")