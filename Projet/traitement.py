from time import sleep
from json import dumps
import pandas as pd 
import numpy as np
from json import loads
import numpy as np
import os
import sys
import findspark
findspark.init()

import pyspark
from pyspark.sql import SQLContext 
sc = pyspark.SparkContext(appName='Projet big data')
sqlc = SQLContext(sc)
path ='/home/mehdi/Downloads/meteo.csv'
if os.path.exists(path):
    df_spark1= sqlc.read.csv(path)
    stats =df_spark1.summary().toPandas()
    stats.head(10)
    stats.to_csv('stats.csv')
else:
    sys.exit()