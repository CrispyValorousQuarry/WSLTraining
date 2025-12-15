from pyspark import SparkContext
sc = SparkContext('local[*]','DataIO')

all_data=sc.textFile('SparkExercises/sales_data*.csv')
header = all_data.first()
data=all_data.filter(lambda line: line!=header)
print(f'Header: {header}')
print(f'Data records: {data.count()}')
print(f'First record: {data.first()}')

#Parse CSV records
def parse_record(line):
    #Parse CSV line into structured data
    parts=line.split(',')
    return{
        'product_id':parts[0],
        'name': parts[1],
        'category':parts[2],
        'price':float(parts[3]),
        'quantity':int(parts[4])
    }

#Parse all records
parsed=data.map(parse_record)
#Calculate revenue for each product
revenue = parsed.map(lambda r: f"{r['product_id']},{r['quantity']:.2f}")

revenue.coalesce(1)
revenue.saveAsTextFile('./SparkExercises/savedFile2')