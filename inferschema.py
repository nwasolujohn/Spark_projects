from pyspark.sql import SparkSession as ss

spark = ss.builder.appName('inferschema').getOrCreate()

people = spark.read.option('header', 'true').option('inferSchema', 'true').csv('fakefriends-header.csv')

print('example of schema')
people.printSchema()

print('selecting columns')
people.select('name', 'age').show()

print('filtering')
people.filter(people.age > 30).show()

print('decreasing the number of ages')
people.select(people.name, people.age - 3).show()

spark.stop()