from app import config
from app.controllers.spark import spark, sc, sqlContext, grid
from app.controllers.main import fs, net as online

from flask import render_template, request, redirect, url_for

import os, re, mimetypes



def createDataFrame(item):
    """
    Create a DataDrame given a file path and format.
    """
    format = item['format']
    path = item['source'] if ('path' not in item) else item['path']
    
    dataframe = None
    
    try:
        if format == 'json':
            dataframe = sqlContext.read.json(path)
            
        elif format == 'xml':
            # TODO: improve XML schema inference
            dataframe = sqlContext.read.format('com.databricks.spark.xml')\
                .option('rowTag', 'doc')\
                .load(path)
            
        elif format == 'csv':
            dataframe = sqlContext.read.format('com.databricks.spark.csv')\
                .option('header', 'true').option('inferSchema', 'true')\
                .load(path)
                
        elif format in ['parquet', 'binary']:
            dataframe = sqlContext.read.parquet(path)
            
        elif format == 'html':
            #script_tag_re = re.compile(r'(<head>.*?</head>)')
            tag_re = re.compile(r'(<!--.*?-->|<[^>]*>)')
            entity_re = re.compile(r'&([^;]+);')
            
            dataframe = sc.wholeTextFiles(path)\
                .mapValues(lambda text: tag_re.sub('',text))\
                .mapValues(lambda text: entity_re.sub('',text))\
                .toDF(['file','text'])
        
        elif format == 'table':
            dbms = item['dbms']
            host = item['host']
            port = item['port']
            database = item['database']
            username = item['username']
            password = item['password']
            table = item['table']
            
            url = "jdbc:"+dbms.lower()+"://"+host+":"+port+"/"+database
            
            dataframe = sqlContext.read.format("jdbc")\
                .option("driver", "com.mysql.jdbc.Driver")\
                .option("url", url)\
                .option("dbtable", table)\
                .option("user", username)\
                .option("password", password).load()
                
        else: # Plain Text File
            #dataframe = sc.textFile(path).map(lambda text: [text]).toDF(['text'])
            #rdd.foreach(temp)
            #dataframe.show()
            #dataframe.printSchema()
            dataframe = sc.wholeTextFiles(path).toDF(['file','text'])
            
    except:
        print 'Error: DataFrame could not be created for resource ' \
            + item.__repr__()
    
    # Rename columns (removing all special characters)
    if dataframe:
        dataframe = renameColumns(dataframe)
    
    return dataframe

def temp(x):
    print x

def saveDataFrame(dataframe, path):
    """
    Save a given DataDrame as a file of the specified format,
    Parquet by default.
    """
    try:
        dataframe.write.mode('overwrite').parquet(path)
    except:
        print 'Error: DataFrame cloud not be saved at ' + path
        return False
    
    return True


def renameColumns(df):
    """
    Alias all column names to avoid errors.
    """
    for column in df.columns:
        new_column = re.sub('\W','',column).lower() # strip all special characters
        df = df.withColumnRenamed(column, new_column)
    
    return df


def load(item):
    """
    Load the file in memory as a new DataFrame.
    """
    df = createDataFrame(item)
    
    if df:
        grid.load(df)
        return True
        
    return False


def join(item):
    """
    Load the file in memory as a new DataFrame and try to merge it
    with data in the DataGrid using a common sub-schema (i.e. shared columns).
    """
    df = createDataFrame(item)
    
    if df:
        grid.merge(df)
        return True
        
    return False


def send_to_hdfs(old_item, new_item):
    """
    Load the given local file in memory as a new DataFrame 
    and store it back to HDFS.
    """
    df = createDataFrame(old_item)
    
    if df:
        saved = saveDataFrame(df, new_item['source'])
        return saved
    
    return False



# -----------------------------------------------------------------------------
# DEPRECATED METHODS
# -----------------------------------------------------------------------------

@spark.route('/query/<filename>', methods=['GET'])
def querySimple(filename=None):
    return query(filename)
    
@spark.route('/query', methods=['GET', 'POST'])
def query(file_name=None):
    
    if file_name is not None:
        query = 'SELECT * FROM ' + file_name
    elif request.method == 'POST':
        query = request.form['query']
    else:
        query = 'SELECT * FROM DataGrid'
    
    df = None
    table = {'schema': {},
             'data': []}
    
    try:
        #query = query.replace(".", "_")
        #df = sqlContext.sql(query)
        df = grid.datagrid.dataframe
        df.registerTempTable('DataGrid')
        df = sqlContext.sql(query)
        schema = df.schema.jsonValue()
        data = df.take(1000) #df.sample(False, 0.1).collect() # retrieve only 10% of the DataFrame
        table = {'schema': schema,
                 'data': data}
        #df.show()
    except:
        table = {'schema': {},
                 'data': []}
        
    return render_template('tables.html', 
                           query=query, 
                           table=table,
                           table_names=sqlContext.tableNames())
