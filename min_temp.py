from unittest import result
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('min_temp')
sc = SparkContext(conf = conf)

def parseline(line):
    rdd = line.split(',')
    station_id = str(rdd[0])
    entry_type = str(rdd[2])
    tmp = float(rdd[3])* 0.1
    return(station_id, entry_type, tmp)


lines = sc.textFile('1800.csv')
stations_info = lines.map(parseline)
# filter takes only the vlaues gotten from the in function
min_tmps = stations_info.filter(lambda x: 'TMIN' in x[1])
min_tmps = min_tmps.map(lambda x: (x[0], x[2]))
min_tmps_station = min_tmps.reduceByKey(lambda x,y: min(x,y))
results = min_tmps_station.collect()

for i in results:
    print (i[0] + "\t{:.2f}C".format(i[1]))

 