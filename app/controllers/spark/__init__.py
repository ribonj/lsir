from app import config
from flask import Blueprint

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

spark = Blueprint('spark', __name__)

# Spark Server
conf = SparkConf()
conf.setMaster(config.SPARK_MASTER_URL)
conf.setAppName(config.APP_NAME)
conf.set('conf.spark.files.overwrite', 'true')

sc = SparkContext(conf=conf)
#sc.addPyFiles('/Users/julien/Development/epfl/lsir/app/controllers/spark/ml.py')

sqlContext = SQLContext(sc)

import default, grid #, pivot
