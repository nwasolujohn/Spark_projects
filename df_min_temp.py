from pyspark.sql import SparkSession as ss, functions as func
from pyspark.sql.types import StructType as st, StructField as sf, StringType as strt, IntegerType as intt, FloatType as ft

spark = ss.builder.appName('df_min_temp').getOrCreate()

schema = st([\
    sf('station_id', strt(), True),\
    sf('date', intt(), True),\
    sf('measure_type', strt(), True),\
    sf('temperature', ft(), True)])

df = spark.read.schema(schema).csv('1800.csv')
# df.createOrReplaceTempView('stations')
# min_temp = spark.sql('SELECT station_id,\
#                              ROUND(min(temperature), 2)*0.1 AS min_temp\
#                              FROM stations\
#                              WHERE measure_type = "TMIN"\
#                              GROUP BY station_id')
# min_temp.show()


min_fields = df.filter(df.measure_type == 'TMIN')

min_stations = min_fields.select('station_id', 'temperature')

min_temp = min_stations.groupBy('station_id').min('temperature')

min_station_c = min_temp.withColumn('temp', func.round(func.col('min(temperature)') * 0.1, 2))\
    .select('station_id', 'temp').sort('temp')



for i in min_station_c.collect():
    print(i[0] + '\t{:.2f}°c'.format(i[1]))



spark.stop()
                                                  


