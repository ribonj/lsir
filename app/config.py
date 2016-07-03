'''
 Configuration
'''

from os.path import join

# Paths
APP_PATH = "."
SPARK_PATH = "/opt/spark/python/"
VENV_PATH = 'venv/bin/activate_this.py'

# Flask
FLASK_HOST = '0.0.0.0'
FLASK_PORT = 5000
DEBUG = True
CONFIG_FILE = "app.config"
CONTROLLER_FOLDER = 'controllers'
MODEL_FOLDER = 'models'
STATIC_FOLDER = 'static'
TEMPLATE_FOLDER = 'views'

ALLOWED_EXTENSIONS = set(['csv', 'json', 'parquet', 'txt', 'html', 'xml'])
ALLOWED_DATABASES = set(['MySQL', 'MongoDB'])
UPLOAD_FOLDER = 'data/documents'

USER_FOLDER = join('data', 'user')
USER_SOURCE_FILE = join(USER_FOLDER, 'sources.json')

# Application
APP_NAME = "Swashbuckler"
DATA_GRID = 'grid'
DATA_SIZE = 1000

# Hadoop
HDFS_HOST = 'localhost'
HDFS_PORT = '9000'
HDFS_USER = 'julien'

HDFS_URL = 'hdfs://' + HDFS_HOST + ':' + HDFS_PORT

HDFS_DATA_URL = HDFS_URL + '/data'
HDFS_DOCS_URL = HDFS_URL + '/data/documents'
HDFS_USER_URL = HDFS_URL + '/user/'+ HDFS_USER

# Spark
SPARK_MASTER_HOST = '127.0.0.1' #'localhost'
SPARK_MASTER_PORT = '7077'
#SPARK_MASTER_URL = 'spark://' + SPARK_MASTER_HOST + ':' + SPARK_MASTER_PORT
SPARK_MASTER_URL = 'local[*]'
# option:
#  - local: Run Spark locally with one worker thread: no parallelism at all.
#  - local[*]: Run Spark locally with as many worker threads as logical cores.
#  - spark://HOST:PORT: Connect to the given Spark standalone cluster master. 
#    (spark://localhost:7077 by default)


# Stanford CoreNLP
CORENLP_HOST = 'localhost'
CORENLP_PORT = '9000'
CORENLP_URL = 'http://' + CORENLP_HOST + ':' + CORENLP_PORT

# HBase
HBASE_HOST = 'localhost'
HBASE_PORT = '2181'
HBASE_TABLE = 'test'

HBASE_CLIENT = "org.apache.hadoop.hbase.client.Result"

HBASE_INPUT_FORMAT = "org.apache.hadoop.hbase.mapreduce.TableInputFormat"
HBASE_OUPUT_FORMAT = "org.apache.hadoop.hbase.mapreduce.TableOutputFormat"

HBASE_KEY = "org.apache.hadoop.hbase.io.ImmutableBytesWritable"
HBASE_VALUE = "org.apache.hadoop.io.Writable"

HBASE_KEY_INPUT_CONVERTER = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
HBASE_VALUE_INPUT_CONVERTER = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"

HBASE_KEY_OUTPUT_CONVERTER = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
HBASE_VALUE_OUTPUT_CONVERTER = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"

HBASE_INPUT_CONF = {"hbase.zookeeper.quorum": HBASE_HOST,
                    "hbase.mapreduce.inputtable": HBASE_TABLE}

HBASE_OUTPUT_CONF = {"hbase.zookeeper.quorum": HBASE_HOST,
                     "hbase.mapred.outputtable": HBASE_TABLE,
                     "mapreduce.outputformat.class": HBASE_OUPUT_FORMAT,
                     "mapreduce.job.output.key.class": HBASE_KEY,
                     "mapreduce.job.output.value.class": HBASE_VALUE}

# Security
#import hashlib
#SECRET_KEY = '7c31bcfa0d1d3c14c7841e078b4ab1fab1c936b1'
#HASH_ALGORITHM = hashlib.sha1
#USER_TIMEOUT = 3600
