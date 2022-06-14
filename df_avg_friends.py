from pyspark.sql import SparkSession as ss
from pyspark.sql import Row, functions as fn



spark = ss.builder.appName('df_avg_friends').getOrCreate()

# def mapper(line):
#     rdd = line.split(',')
#     return Row(ID = int(rdd[0]),\
#         name = str(rdd[1].encode('utf-8')),\
#         age = int(rdd[2]),\
#         num_friends = int(rdd[3])
#     )
    

# lines = spark.sparkContext.textFile('fakefriends.csv')
# people = lines.map(mapper)

# people_schema = spark.createDataFrame(people).cache()
# people_schema.createOrReplaceTempView('people')

# avg_friends = spark.sql('\
#     SELECT age, \
#     ROUND(AVG(num_friends), 2) AS avg_friends\
#     FROM people \
#     GROUP BY age \
#     ORDER BY avg_friends'
# )


# for i in avg_friends.collect():
#     print(i)


lines = spark.read.option('header', 'true').option('inferSchema', 'true').csv('fakefriends-header.csv')
people = lines.select('age', 'friends')
people.groupBy('age').agg(fn.round(fn.avg('friends'), 2).alias('avg_friends')).sort('avg_friends').show()


spark.stop()

