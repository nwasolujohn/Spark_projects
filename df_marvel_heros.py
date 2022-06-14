from pyspark.sql import SparkSession as ss, functions as func
from pyspark.sql.types import StructType as st, StructField as sf, IntegerType as intt, StringType as strt

spark = ss.builder.appName('marvel_heros').getOrCreate()

schema = st([sf('id', intt(), True),\
             sf('name', strt(), True)])

df = spark.read.schema(schema).option('sep', ' ').csv('Marvel Names.txt')
# df.printSchema()
lines = spark.read.text('Marvel Graph.txt')
connections = lines.withColumn('id', func.split(func.trim(func.col('value')), ' ')[0])\
                  .withColumn('connections', func.size(func.split(func.trim(func.col('value')), ' ')) -1)\
                  .groupBy('id').agg(func.sum('connections').alias('connections'))


# heros = df.join(connections).where(df["id"] == connections["id"])
heros = df.join(connections,["id"])
heros.sort(func.col("connections").desc()).show(20)
heros.filter('connections = 1').show(20)



spark.stop()

