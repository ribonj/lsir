# Simple program illustrating how to use Spark with Python
import sys
from app import config

sys.path.append(config.SPARK_PATH)
from pyspark import SparkConf, SparkContext

# default is hdfs://localhost:9000/user/julien/input/mapred-queues.xml.template (instead of file:/)
input = config.HDFS_URL + "input/mapred-queues.xml.template"
output = config.HDFS_URL + "output"
    
conf = SparkConf()
conf.setMaster(config.SPARK_MASTER_URL)
conf.setAppName(config.APP_NAME)
sc = SparkContext(conf=conf)
#sc = SparkContext(config.SPARK_MASTER_URL, config.APP_NAME)

data = sc.textFile(input).cache() # cache is optional


words = data.flatMap(lambda line: line.split(' '))

wordCounts = words.map(lambda word: (word, 1)) \
    .reduceByKey(lambda c1,c2: c1+c2) \

for (word, count) in wordCounts.collect():
    print "(%s: %i)" % (word, count)

#wordCounts.saveAsTextFile(output)

sc.stop()
