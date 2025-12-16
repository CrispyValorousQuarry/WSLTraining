from pyspark import SparkContext

sc = SparkContext("local[*]", "RDDBasics")

# 1. Create RDD from a Python list
numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
print(f"Numbers: {numbers.collect()}")
print(f"Partitions: {numbers.getNumPartitions()}")

# 2. Create RDD with explicit partitions
# YOUR CODE: Create the same list with exactly 4 partitions
numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10],4)
print(f"Numbers: {numbers.collect()}")
print(f"Partitions: {numbers.getNumPartitions()}")

# 3. Create RDD from a range
# YOUR CODE: Create RDD from range(1, 101)
# Given: numbers RDD [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
numbers=sc.range(1,101)
print(f"Numbers: {numbers.collect()}")
print(f"Partitions: {numbers.getNumPartitions()}")

# Task A: Square each number
# Expected: [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]

numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
squared = numbers.map(lambda x:x**2)
print(f"Numbers: {squared.collect()}")
print(f"Partitions: {squared.getNumPartitions()}")

# Task B: Convert to strings with prefix
# Expected: ["num_1", "num_2", "num_3", ...]
prefixed = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
prefixed=prefixed.map(lambda x:'num_'+str(x))
print(f"Numbers: {prefixed.collect()}")
print(f"Partitions: {prefixed.getNumPartitions()}")

# Task A: Keep only even numbers
# Expected: [2, 4, 6, 8, 10]
evens = numbers.filter(lambda x:x%2==0)
print(f"Numbers: {evens.collect()}")
print(f"Partitions: {evens.getNumPartitions()}")

# Task B: Keep numbers greater than 5
# Expected: [6, 7, 8, 9, 10]
greater_than_5 = numbers.filter(lambda x:x>5)
print(f"Numbers: {greater_than_5.collect()}")
print(f"Partitions: {greater_than_5.getNumPartitions()}")

# Task C: Combine - even AND greater than 5
# Expected: [6, 8, 10]
combined = numbers.filter(lambda x:x>5 and x%2==0)
print(f"Numbers: {combined.collect()}")
print(f"Partitions: {combined.getNumPartitions()}")

# Given sentences
sentences = sc.parallelize([
    "Hello World",
    "Apache Spark is Fast",
    "PySpark is Python plus Spark"
])

# Task A: Split into words (use flatMap)
# Expected: ["Hello", "World", "Apache", "Spark", ...]
words = sentences.flatMap(lambda line: line.split(' '))
print(words.collect())

# Task B: Create pairs of (word, length)
# Expected: [("Hello", 5), ("World", 5), ...]
word_lengths = words.flatMap(lambda word: (word,len(word)))
print(word_lengths.collect())
sc.stop()