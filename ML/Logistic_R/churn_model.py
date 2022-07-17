import func_corr
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, year, month
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

spark = SparkSession.builder.appName('churn_model').master('local[*]').getOrCreate()

df = spark.read.option('inferSchema', 'true').option('header', 'true').csv('customer_churn.csv')

# the acount manager was randomly generated and will not be usefull for the model
# the company and address column is very distinct and can not be useful in the model
df_feat = df.select('Age', 'Total_Purchase', 'Years', 'Account_Manager', 'Num_Sites', year(to_date\
    (col("Onboard_date"),"yyyy-MM-dd HH:mm:ss")).alias("Year"), \
    month(to_date(col("Onboard_date"),"yyyy-MM-dd HH:mm:ss")).alias("Month"), 'Churn')

corr = func_corr.correaltion(df_feat)
corr.show()

# from the correlation test, only two features are remotely correlated to the target lable churn
# given this, the model will only have two features

model_col = df_feat.select('Years', 'Num_Sites', 'Churn')

assembler = VectorAssembler(inputCols = ['Years', 'Num_Sites'], outputCol = 'features')
feat_col = assembler.transform(model_col)
feat_col = feat_col.select('features', 'Churn')
train, test = feat_col.randomSplit([0.7, 0.3], seed = 19)

log_model = LogisticRegression(labelCol = 'Churn')
fitted_model = log_model.fit(train)
results = fitted_model.transform(test)

training_sum = fitted_model.summary
training_sum.predictions.describe().show()
model_eval = BinaryClassificationEvaluator(rawPredictionCol = 'prediction', labelCol = 'Churn').evaluate(results)
fitted_model.evaluate(test).predictions.show()
print('Area under ROC :', model_eval)

spark.stop()
