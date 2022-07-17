from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

spark = SparkSession.builder.appName('log_model').getOrCreate()
df = spark.read.format('libsvm').load('sample_libsvm_data.txt')

train, test = df.randomSplit([0.7, 0.3], seed = 53)

log_model = LogisticRegression()
fitted_model = log_model.fit(train)  
prediction = fitted_model.evaluate(test)
prediction.predictions.show()

model_eval = BinaryClassificationEvaluator()
model_eval_roc = model_eval.evaluate(prediction.predictions)
print(model_eval_roc)