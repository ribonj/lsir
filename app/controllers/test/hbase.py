from app.controllers.test import test
from app import config, sparkContext as sc

# Before executing this code, a table called 'test' with column preficx 'f1'
# must be created in HBase.


@test.route("/hbase_in")
def hbase_input():
    hbaseRDD = sc.newAPIHadoopRDD(
        config.HBASE_INPUT_FORMAT,
        config.HBASE_KEY,
        config.HBASE_CLIENT,
        keyConverter=config.HBASE_KEY_INPUT_CONVERTER,
        valueConverter=config.HBASE_VALUE_INPUT_CONVERTER,
        conf=config.HBASE_INPUT_CONF)
    
    hbaseRDD = hbaseRDD.flatMapValues(lambda v: v.split("\n"))
    
    output = hbaseRDD.collect()
    for (k,v) in output:
        print (k,v)
    
    return "ok"


@test.route("/hbase_out")
def hbase_output():
    hbaseRDD = sc.parallelize([['row5', 'f1', 'q1', 'value5'],
                               ['row6', 'f1', 'q1', 'value6']])
    hbaseRDD = hbaseRDD.map(lambda x: (x[0], x))
    
    hbaseRDD.saveAsNewAPIHadoopDataset(
       conf=config.HBASE_OUTPUT_CONF,
       keyConverter=config.HBASE_KEY_OUTPUT_CONVERTER,
       valueConverter=config.HBASE_VALUE_OUTPUT_CONVERTER)
    
    return "ok"
