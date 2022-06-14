from pyspark.sql import SparkSession as ss, functions as func
from pyspark.sql.types import StructType as st, StructField as sf, IntegerType as intt, FloatType as ft

spark = ss.builder.appName('df_purchase_value').getOrCreate()

schema = st([\
    sf('customer_id', intt(), True),\
    sf('item_id', intt(), True),\
    sf('cost', ft(), True)\
])

df = spark.read.schema(schema).csv('customer-orders.csv')
# df.createOrReplaceTempView('orders')
# customer_value = spark.sql('SELECT customer_id, ROUND(SUM(cost), 2) AS c_value FROM orders GROUP BY customer_id ORDER BY c_value')
# customer_value.show()

customer_info = df.select('customer_id', 'cost')
customer_value = customer_info.groupBy('customer_id').agg(func.round(func.sum('cost'), 2).alias('total_spent'))
customer_value.show()

spark.stop()  