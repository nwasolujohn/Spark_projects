import re
from unittest import result
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('word_count')
sc = SparkContext(conf = conf)


def normalise_words(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

input = sc.textFile('book.txt')
words = input.flatMap(normalise_words)
word_count = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
sorted_word_count = word_count.map(lambda x: (x[1], x[0])).sortByKey()
results = sorted_word_count.collect()

for i in results:
    count = str(i[0])
    word = i[1].encode('ascii', 'ignore')
    if (word):
        print (word.decode() + ":\t\t" + count)
