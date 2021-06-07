from time import sleep
from json import dumps
import pandas as pd 
import numpy as np
import numpy as np
import findspark
findspark.init()

import pyspark
sc = pyspark.SparkContext(appName='Projet big data')
from pyspark.sql import SQLContext 
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
sqlc = SQLContext(sc)
df_spark1 = df_spark1= sqlc.read.csv("meteo.csv")
df_train, df_test = df_spark1.randomSplit([0.8, 0.2], seed=78)
stages = []
cols = df_spark1.columns
numericCols = ["dt","temp_min","temp_max","temperature","humidité","pressure","wind_speed","wind_deg"]
label_stringIdx = StringIndexer(inputCol = 'weather', outputCol = 'label')
stages += [label_stringIdx]
assembler = VectorAssembler(inputCols=numericCols, outputCol="features")
stages += [assembler]
lr = LogisticRegression(featuresCol='features', labelCol='label', regParam=0.3)
stages +=[lr]
pipeline = Pipeline(stages = stages)
pipeline_model = pipeline.fit(df_train)
df_pred = pipeline_model.transform(df_test)
bin_evaluator = BinaryClassificationEvaluator(metricName='areaUnderROC')
print(f'area under ROC curve: {bin_evaluator.evaluate(df_pred)}')
multi_evaluator = MulticlassClassificationEvaluator(metricName='accuracy')
print(f'accuracy: {multi_evaluator.evaluate(df_pred)}')
