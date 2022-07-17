from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseline(line):
    rdd = line.split(',')
    age = int(rdd[2])
    num_friends = int(rdd[3])
    return (age, num_friends)

lines = sc.textFile("fakefriends.csv")
rd = lines.map(parseline)
age_group = rd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
avg_friends_by_age = age_group.mapValues(lambda x: x[0] / x[1])
results = avg_friends_by_age.collect()
for i in results:
    print (i)