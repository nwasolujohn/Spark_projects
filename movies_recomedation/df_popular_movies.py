from pyspark.sql import SparkSession as ss


spark = ss.builder.appName('df_popular_movies').getOrCreate()
df = spark.read.option('header', 'true').option('inferSchema', 'true').csv('ml-latest/ratings.csv')
df1 = spark.read.option('header', 'true').option('inferSchema', 'true').csv('ml-latest/movies.csv')

# top_movies = df.groupBy('movieId').count().orderBy(func.desc('count'))
# top_movies.show(15)

df.createOrReplaceTempView('movie_ratings')
df1.createOrReplaceTempView('movie_names')
top_movies = spark.sql('SELECT mr.movieId,\
                               mn.title,\
                               count(mr.movieId) As movie_count\
                        FROM movie_ratings mr\
                        INNER JOIN movie_names mn\
                               ON mr.movieId = mn.movieId\
                        GROUP BY mr.movieId, mn.title\
                        ORDER BY movie_count DESC\
                        LIMIT 10')
top_movies.show()





spark.stop()