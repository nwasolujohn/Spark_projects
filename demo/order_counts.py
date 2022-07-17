from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('order_counts')
sc = SparkContext(conf = conf)

def parseline(line):
    rdd = line.split(',')
    customer_id = int(rdd[0])
    # item_id = rdd[1]
    cost = float(rdd[2])
    return(customer_id, cost)

lines = sc.textFile('customer-orders.csv')
customer_info = lines.map(parseline)
customer_expenditure = customer_info.reduceByKey(lambda x, y: x + y)
customer_expenditure_sorted = customer_expenditure.map(lambda x: (x[1], x[0])).sortByKey()
results = customer_expenditure_sorted.collect()

for i in results:
    customer_id = i[1]
    cost = i[0]
    print(str(customer_id) + ':',  "\t{:.2f}".format(cost))