from pyspark.sql import SparkSession as ss
from pyspark.sql import Row

spark = ss.builder.appName('SparkSQL').getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID = int(fields[0]), \
        name = str(fields[1].encode('utf-8')), \
        age = int(fields[2]), \
        num_friends = int(fields[3])
    )

lines = spark.sparkContext.textFile('fakefriends.csv')
people = lines.map(mapper)

people_schema = spark.createDataFrame(people).cache()
people_schema.createOrReplaceTempView('people')

teenagers = spark.sql('SELECT name, age FROM people WHERE age BETWEEN 13 AND 19')
for i in teenagers.collect():
    print(i)

spark.stop()