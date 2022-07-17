from pyspark.sql import SparkSession as ss, functions as func

spark = ss.builder.appName('df_word_count').getOrCreate()


lines = spark.read.text('book.txt')
words = lines.select(func.explode(func.split(lines.value, '\\W+')).alias('word'))
words.filter(words.word != "")
words_lower = words.select(func.lower(words.word).alias('word'))
# words_lower.createOrReplaceTempView('words')
# count_words = spark.sql('SELECT word, COUNT(word) AS cnt\
#     from words \
#     GROUP BY word \
#     ORDER BY cnt DESC')
word_count = words_lower.groupBy('word').count()
sort_word_count = word_count.sort('count')
sort_word_count.show()
# sort_word_count.show(sort_word_count.count())

# count_words.show()

spark.stop()