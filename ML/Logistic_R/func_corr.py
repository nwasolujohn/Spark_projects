from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('churn_model').master('local[*]').getOrCreate()

def correaltion(features):
    corr_col = 'vect_col'
    assembler = VectorAssembler(inputCols = features.columns, outputCol = corr_col)
    assembler_vect = assembler.transform(features).select(corr_col)
    matrix = Correlation.corr(assembler_vect, corr_col).collect()[0][0]
    corr_matrix = matrix.toArray().tolist()
    df_corr = spark.createDataFrame(corr_matrix, features.columns)

    return df_corr
