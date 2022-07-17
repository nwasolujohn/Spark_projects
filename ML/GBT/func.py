from pyspark.ml import Pipeline
from pyspark.sql.functions import udf
from pyspark.ml.stat import Correlation
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler

def norm_df(df):
    # # UDF for converting column type from vector to double type
    unlist = udf(lambda x: round(float(list(x)[0]),4), DoubleType())
    for i in ['N', 'P_int', 'VVT_in_crk', 'VVT_ex_crk', 'N_t', 'P_ext', 'VSP', "HC"]:
        # VectorAssembler Transformation - Converting column to vector type
        assembler = VectorAssembler(inputCols=[i],outputCol=i+"_Vect")

        # MinMaxScaler Transformation
        scaler = MinMaxScaler(inputCol=i+'_Vect', outputCol=i+'_norm')

        # Pipeline of VectorAssembler and MinMaxScaler
        pipeline = Pipeline(stages=[assembler, scaler])

        # Fitting pipeline on dataframe
        df = pipeline.fit(df).transform(df).withColumn(i+'_norm', unlist(i+'_norm')).drop(i+'_Vect')
    norm_df = df.select('N_norm', 'P_int_norm', 'VVT_in_crk_norm', 'VVT_ex_crk_norm', \
        'N_t_norm', 'P_ext_norm', 'VSP_norm', "HC_norm")
    return norm_df

def corr(df):
    # convert columns to vector first
    vector_col = 'corr_features'
    assembler = VectorAssembler(inputCols=df.columns, outputCol=vector_col)
    df_vector = assembler.transform(df).select(vector_col)

    # get correlation matrix
    pearsonCorr = Correlation.corr(df_vector, vector_col, 'pearson').collect()[0][0]
    print(pearsonCorr)