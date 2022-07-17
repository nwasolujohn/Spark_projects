from func import norm_df
from pyspark.sql import SparkSession as ss
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorIndexer, VectorAssembler 
from pyspark.ml.regression import GBTRegressor 
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator 


spark = ss.builder.appName('emission_ml').master('local[*]').getOrCreate()
zero = spark.read.option('header', 'true').option('inferschema', 'true').csv('zero.csv')

# Caching the DataFrame in memory. This improves performance since subsequent calls to the DataFrame 
# can read from memory instead of re-reading the data from disk.
# Normalizing the features
norm_zero = norm_df(zero)
norm_zero.cache()

# Spliting the data into train and test dataset
# seed ensures the same radom split is gotten everytime the model is ran
train_HC, test_HC = norm_zero.randomSplit([0.7, 0.3], seed = 19)

# Remove the target column from the input feature set.
featuresCols = norm_zero.columns
featuresCols.remove('HC_norm')

## Training the model 

# Most MLlib algorithms require a single input column containing a vector of features and a single target column.
# vectorAssembler combines all feature columns into a single feature vector column, "rawFeatures".
vectorAssembler = VectorAssembler(inputCols=featuresCols, outputCol="rawFeatures")
 
# vectorIndexer identifies categorical features and indexes them, and creates a new column "features". 
vectorIndexer = VectorIndexer(inputCol="rawFeatures", outputCol="features", maxCategories=2)

## Definnning the model

# The next step is to define the model training stage of the pipeline. 
# The following command defines a GBTRegressor model that takes an input column "features" by default and learns to predict the labels in the "cnt" column. 
gbt = GBTRegressor(labelCol="HC_norm")


# The third step is to wrap the model you just defined in a CrossValidator stage. 
# CrossValidator calls the GBT algorithm with different hyperparameter settings. 
# It trains multiple models and selects the best one, based on minimizing a specified metric. 
# In this example, the metric is root mean squared error (RMSE).

# Define a grid of hyperparameters to test:
#  - maxDepth: maximum depth of each decision tree 
#  - maxIter: iterations, or the total number of trees 
paramGrid = ParamGridBuilder()\
  .addGrid(gbt.maxDepth, [10, 13, 16])\
  .addGrid(gbt.maxIter, [100, 200, 300])\
  .addGrid(gbt.stepSize, [0.03, 0.5, 0.7])\
  .build()

# Define an evaluation metric.  The CrossValidator compares the true labels 
# with predicted values for each combination of parameters, and calculates this value to determine the best model.
evaluator = RegressionEvaluator(metricName="rmse", labelCol=gbt.getLabelCol(), predictionCol=gbt.getPredictionCol())
 
# Declare the CrossValidator, which performs the model tuning.
cv = CrossValidator(estimator=gbt, evaluator=evaluator, estimatorParamMaps=paramGrid, numFolds=5)

## Creating a pipeline

pipeline = Pipeline(stages=[vectorAssembler, vectorIndexer, cv])

# Training the pipeline with a single call. Calling fit(), the pipeline runs feature processing, model tuning, 
# and training and returns a fitted pipeline with the best model it found.
model = pipeline.fit(train_HC)

# printing the best parameters for future reff


# The transform() method of the pipeline model applies the full pipeline to the input dataset. 
# The pipeline applies the feature processing steps to the dataset and then uses the fitted GBT 
# model to make predictions. The pipeline returns a DataFrame with a new column predictions.
predictions = model.transform(test_HC)
predictions.select("HC_norm", "prediction", *featuresCols).show(20)

rmse = evaluator.evaluate(predictions)
print("RMSE on our test set: %g" % rmse)

spark.stop()