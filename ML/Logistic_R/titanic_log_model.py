from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler, OneHotEncoder, StringIndexer

spark = SparkSession.builder.appName('titanic_mdoel').getOrCreate()

df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load('titanic.csv')
df.columns

# these are the only columns useful for this sample model
df_col = df.select(['Survived', 'Pclass', 'Sex', 'Age', 'SibSp', 'Parch', 'Fare', 'Embarked'])

#for the missing data, to keep it simple the missing data will be dropped
df_col = df_col.na.drop()

# transforming the gender column to categorical indexe
gender_indexer = StringIndexer(inputCol = 'Sex', outputCol = 'SexIndexer')
embarked_indexer = StringIndexer(inputCol = 'Embarked', outputCol = 'EmbarkedIndexer')

# transform the indexes to a one hot encoding
gender_encoder = OneHotEncoder(inputCol = 'SexIndexer', outputCol = "SexVect")
embarked_encoder = OneHotEncoder(inputCol = 'EmbarkedIndexer', outputCol = "EmbarkedVect")

# creating an assemnbler
assembler = VectorAssembler(inputCols = ['Pclass', 'SexVect', 'Age', 'SibSp', 'Parch', 'Fare', 'EmbarkedVect'],\
                                        outputCol = 'features')
                
log_reg_titanic = LogisticRegression(featuresCol = 'features', labelCol = 'Survived')

# creating a pipeline 
pipeline = Pipeline(stages=[gender_indexer, embarked_indexer, gender_encoder, embarked_encoder, assembler, log_reg_titanic])
train, test = df_col.randomSplit([0.7, 0.3], seed = 19)
fit_model = pipeline.fit(train)
# whenever a transform is called on a test data, the outputcol for prediction is always labeled prediction
results = fit_model.transform(test)
model_eval = BinaryClassificationEvaluator(rawPredictionCol = 'prediction', labelCol = 'Survived')

auc = model_eval.evaluate(results)

print('Area under ROC :', auc)


