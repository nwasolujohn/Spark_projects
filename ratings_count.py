from pyspark import SparkConf as con, SparkContext as contxt
import collections

conf = con().setMaster('local').setAppName('histogram_ratings')
sc = contxt(conf = conf)

lines = sc.textFile('ml-100k/u.data')
# the third column from the imported file is the rating coiumn
# We will create a new variable and pass the ratings to it²
ratings = lines.map(lambda x: x.split()[2])
# the ratings will group by ratings and counted
ratings_count = ratings.countByValue()

results_sorted = collections.OrderedDict(sorted(ratings_count.items()))
for key, value in results_sorted.items():
    print('%s %i' % (key, value))
