from app import config
from app.controllers.main import fs
from app.controllers.spark import spark, sc, sqlContext, ml # ML Controller
from app.controllers.spark.helper import extract_sparse_vector
from app.model.datagrid import DataGrid

from pyspark.sql.types import *
from pyspark.sql.functions import udf, when, col, coalesce

from flask import render_template, request, redirect, url_for

import os, nltk, string, re, math
#from hdfs.ext import dataframe
 
 
# DataGrid initialized with an empty DataFrame and empty schema
emptyDataFrame = sqlContext.createDataFrame(sc.emptyRDD(), StructType([]))
datagrid = DataGrid(emptyDataFrame)


@spark.route('/grid/')
def display():
    """
    Render DataGrid using the template engine
    """
    df = datagrid.dataframe
    schema = sorted(df.dtypes, key=lambda t: t[0].lower()) # sort by column name
    data = df.take(100) # limits number of data entries
    
    '''
    if config.DEBUG:
        df.printSchema()
        df.show()
    '''
    return render_template('grid.html', schema=schema, data=data)


def load(dataframe):
    """
    Replaces the current DataGrid with a new DataFrame.
    """
    datagrid.reset()
    return merge(dataframe)


def merge(dataframe):
    """
    Combines the given DataFrame with the existing DataGrid.
    This operation is equivalent to a FULL OUTER JOIN, except that if no match
    is found for a given row, a UNION ALL is performed instead.
    Missing columns in both the DataFrame and DataGrid are added 
    before executing the union operation.
    """
    df = datagrid.merge(dataframe)
    df = datagrid.sample(df)
    df = partition(df)
    datagrid.setDataFrame(df)
    
    print '\n COUNT='+str(df.count())+'\n'
    return display()


def partition(df):
    """
    Partition according to the number of rows (Rule of thumb).
    Use only one partition if number of rows is smaller than a threshold.
    """
    #max_partition = 4
    #partition_count = df.count()/100000 + 1
    if df.count()<100000:
        df = df.coalesce(1)
    return df
    

@spark.route('/grid/save')
def save():
    """
    Save the current DataGrid as parquet file.
    Override parquet file if it already exists.
    """
    location = request.args.get('location')
    name = request.args.get('name')
    format = request.args.get('format')
    
    file_name = name + '.' + format
    
    writer = datagrid.dataframe.write.mode('overwrite')
    
    if location == 'hdfs':
        hdfs_path = os.path.join(config.HDFS_DOCS_URL, file_name)
        free = fs.findFreeName(hdfs_path)
        file_name = free['name']
        hdfs_path = free['source']
        print 'hdfs_path=' + hdfs_path
        
        try:
            #df.write.mode('overwrite').format('parquet').save('hdfs://...')
            writer.save(hdfs_path)

            fs.createItem({
                'name':file_name,
                'type': 'File (HDFS)',
                'format':'binary',
                'size':{'measure':'-', 'unit':''},
                'count':None,
                'source':hdfs_path
            })
        except:
            print 'Error: Could not save to HDFS:' + hdfs_path
    else:
        file_path = os.path.join(config.UPLOAD_FOLDER, file_name)
        file_path = os.path.abspath(file_path)
        free = fs.findFreeName(file_path)
        file_name = free['name']
        file_path = free['source']
        
        writer.save(file_path)
        
        fs.createItem({
            'name':file_name,
            'type': 'File',
            'format':'binary',
            'size':fs.computeReadableSize(os.stat(file_path).st_size),
            'count':None,
            'source':file_path
        })
    
    return display()


@spark.route('/grid/clear')
def clear():
    """
    Clear the whole DataGrid by setting an empty DataFrame.
    """
    datagrid.reset()
    return display()


@spark.route('/grid/undo')
def undo():
    """
    Cancel previous user action.
    """
    datagrid.undo()
    return display()


@spark.route('/grid/redo')
def redo():
    """
    Re-applies cancelled user action.
    """
    datagrid.redo()
    return display()



@spark.route('/grid/add_column/')
def add_column():
    """
    Add a new column to the datagrid.
    """
    name = request.args.get('name') # column name
    value = request.args.get('value') # default value for the entire column
    
    if name and value:
        datagrid.addColumn(name, value)
        
    return display()


@spark.route('/grid/<column>/rename')
def rename(column):
    """
    Rename the given column with new name.
    """
    name = request.args.get('name')
    
    df = datagrid.dataframe
    if column in df.columns:
        df = df.withColumnRenamed(column, name)
        datagrid.setDataFrame(df)
        
    return display()


@spark.route('/grid/<column>/drop')
def drop(column):
    """
    Drops the given column from the DataGrid.
    """
    df = datagrid.dataframe
    if column in df.columns:
        df = df.drop(column)
        datagrid.setDataFrame(df)
    
    return display()


@spark.route('/grid/<column>/expand')
def expand(column):
    """
    Expand vector into multiple column.
    """
    df = datagrid.dataframe
    columns = dict(df.dtypes)
    
    if (column in columns) and (columns[column] == 'vector'):
        dim_count = 2
        
        for dim in range(0, dim_count):
            # converts mllib.Vector --> numpy.array --> list
            # and extracts each dimension individually
            vectorUDF = udf(lambda vec: vec.values.tolist()[dim], DoubleType())
            df = df.withColumn('dim'+str(dim), vectorUDF(df[column]))
        
        df = df.drop(column)
        datagrid.setDataFrame(df)
    
    return display()


@spark.route('/grid/<column>/convert')
def convert(column):
    """
    Convert column type to string.
    This is especially useful to apply textual operations on the column.
    """
    df = datagrid.dataframe
    columns = dict(df.dtypes) # list of tuples (column, type)
    
    if (column in columns):
        # Cast to string if not null, otherwise convert NULL into empty string.
        df = df.withColumn(column, when(col(column).isNull(), '')\
            .otherwise(df[column].cast('string')))
        datagrid.setDataFrame(df)
    
    return display()


@spark.route('/grid/<column>/sort')
def sort(column):
    """
    Sorts DataGrid according to a given column and order.
    """
    order = request.args.get('order')
    df = datagrid.dataframe
    
    # Sort only if column is in DataGrid and 
    if column in df.columns and order:
        if order == 'desc':
            ascending = False
        else: # order == 'asc'
            ascending = True
        
        df = df.sort(column, ascending=ascending)
        datagrid.setDataFrame(df)
        
    return display()


@spark.route('/grid/<column>/filter')
def filter(column):
    """
    Filters out rows from GridData that do not match the given predicate.
    The predicate is constructed based on the given column, operator and value.
    """
    op = request.args.get('op') # operator
    value = request.args.get('val')
    
    df = datagrid.dataframe
    columns = dict(df.dtypes) # list of tuples (column, type)
    
    if (column in columns) and op:
        type = columns[column]
        
        # Operation on textual values
        if type == 'string':
            if op == 'like':
                value = '%'+value+'%'
            value = '"'+value+'"'
        
        if op in ['is null', 'is not null']:
            value = ''
        
        df = df.filter(column+' '+op+' '+value)
        datagrid.setDataFrame(df)
    
    return display()

@spark.route('/grid/<column>/split')
def split(column):
    """
    Splits the given column in 2 or more columns starting from the beginning 
    or end of the text contained in the column.
    The split is based either on a given length (i.e. number of characters) 
    or on specific characters (e.g. comma "," or semi-colon ";") where
    several characters can be specified at a time.
    """
    split_in = request.args.get('in') # number of splits (2, 3, 4, etc.)
    start_from = request.args.get('from') # start of split (begin or end)
    method = request.args.get('method') # character-based or length based
    chars = request.args.get('info') # series of character or a number
    
    df = datagrid.dataframe
    columns = dict(df.dtypes) # list of tuples (column, type)
    
    if (column in columns) and split_in and start_from and method and chars:
        split_count = int(split_in)
        splitUDF = None
        
        # Depending on the given input parameters, 
        # define a user-defined function that splits the given column.
        for i in range(0,split_count):
            if method=='char' and start_from == 'begin':
                # Split from left using characters 
                splitUDF = udf(
                    lambda r: r.split(chars,split_count-1)[i]
                        if i < len(r.split(chars,split_count-1)) else None, 
                    StringType())
            elif method=='char' and start_from == 'end':
                # Split from right using characters 
                split = udf(
                    lambda r: r.rsplit(chars,split_count-1)[i]
                        if i < len(r.rsplit(chars,split_count-1)) else None, 
                    StringType())
            elif method=='length' and start_from == 'begin' and chars.isdigit():
                # Split from left using length (number of characters) 
                n = int(chars)
                splitUDF = udf(
                    lambda r: r[i*n:min((i+1)*n,len(r))] 
                        if i*n < len(r) else None, 
                    StringType())
            elif method=='length' and start_from == 'end' and chars.isdigit():
                # Split from right using length (number of characters) 
                n = int(chars)
                splitUDF = udf(
                    lambda r: r[max(len(r)-(i+1)*n,0):len(r)-i*n] 
                        if i*n < len(r) else None, 
                    StringType())
            
            # Add a new column with the above UDF
            if splitUDF is not None:
                k = i + 1
                new_column = column+str(k)
                # Search non-existing column name in DataFrame
                while new_column in df.columns:
                    k = k + 1
                    new_column = column+str(k)
                    
                # Replaces NULL values by empty strings;
                # otherwise, applies the split function
                #df = df.withColumn(
                #    new_column, 
                #    when(col(column).isNull(), '')\
                #        .otherwise(splitUDF(df[column])))
                
                df = df.withColumn(new_column, splitUDF(df[column]))
        
        # Drop the original column and set the new DataFrame
        if splitUDF is not None:
            df = df.drop(column)
            datagrid.setDataFrame(df) 
    
    return display()


@spark.route('/grid/<column>/stats')
def computeStatistics(column):
    """
    Obtains a wide variety of statistics in on pass over the a given column.
    The column must contain vectors, i.e. RDD of Vectors.
    """
    df = datagrid.dataframe
    columns = dict(df.dtypes)
    
    if column in columns:
        type = columns[column]
        
        summary = df.describe(column)
        summary = summary.withColumnRenamed('summary', 'Statistics')
        summary = summary.withColumnRenamed(column, 'Value')
        summary_columns = summary.columns
        summary_data = summary.collect() # list of Row
        summary_data = map(lambda r: r.asDict(), summary_data)

        # Build buckets to compute histogram on column values
        count = 1
        min_value = 0
        max_value = 1
        step = 1
        max_count = 20
        
        # extract min, max and count from statistics
        for row in summary_data:
            stat = row['Statistics']
            val = row['Value']
            if stat is not None and val is not None:
                if stat == 'count':
                    count = long(val)
                elif stat == 'min':
                    min_value = val
                elif stat == 'max':
                    max_value = val
        
        max_count = min(max_count, count)
        buckets = []
        tick = []
        format = 'string'
        prefix = ''
        
        # round can convert to integer values
        if type in [ByteType().simpleString(), 
                    ShortType().simpleString(), 
                    IntegerType().simpleString(), 
                    LongType().simpleString()]:
            # Whole Numbers
            min_value = long(min_value)-1
            max_value = long(max_value)+1
            step = max(long(round((max_value-min_value)/max_count)),1)
            buckets = [b for b in range(min_value, max_value, step)]
            format = 'integer'
            
            df = df.map(lambda r: r[column])
            
        elif (type in [FloatType().simpleString(), DoubleType().simpleString()]
              or type[:7] == DecimalType().simpleString()[:7]):
            # Decimal Numbers
            min_value = float(min_value)-1
            max_value = float(max_value)+1
            step = (max_value-min_value)/max_count
            buckets = [min_value+i*step for i in range(0, max_count+1)]
            format = 'decimal'
            
            df = df.map(lambda r: r[column])
        
        elif (type in [StringType().simpleString()]):
            # String Types
            # Remove prefix (if any) and classify text according to the most
            # significant character.
            prefix = os.path.commonprefix([min_value, max_value])
            min_char = ord(min_value[len(prefix)])
            max_char = ord(max_value[len(prefix)])
            
            step = max(int(round(max_char-min_char)/max_count),1)
            axis = [prefix+chr(c) for c in range(min_char, max_char+1, step)]
            buckets = [c for c in range(min_char, max_char+1, step)]
            format = 'string'
            
            df = df.map(lambda r: ord(r[column].encode('utf-8')[len(prefix)]))
        
        if buckets:
            histogram = df.histogram(buckets)
            return render_template(
                'column.html', 
                name=column, type=type,
                statistics=summary_columns, values=summary_data,
                axis=histogram[0], data=histogram[1],
                format=format, step=step, prefix=prefix)
            
    return display()



### ------------- Calls to ML API -------------- ###

@spark.route('/grid/<column>/preprocess')
def preprocess(column):
    df = datagrid.dataframe
    
    if column in df.columns:
        df = ml.preprocess(df, column)
        datagrid.setDataFrame(df)
        
    return display()


@spark.route('/grid/<column>/tokenize')
def tokenize(column):
    df = datagrid.dataframe
    
    if column in df.columns:
        df = ml.tokenize(df, column)
        datagrid.setDataFrame(df)
        
    return display()


@spark.route('/grid/<column>/entity_recognition')
def entity_recognition(column):
    df = datagrid.dataframe
    
    if column in df.columns:
        df = ml.entity_recognition(df, column)
        datagrid.setDataFrame(df)
        
    return display()


@spark.route('/grid/<column>/word2vec')
def word2vec(column):
    """
    Compute Word2Vec of a token contained in a column.
    The column must contain a list of string.
    """
    df = datagrid.dataframe
    if column in df.columns: 
        df = ml.word2vec(df, column)
        datagrid.setDataFrame(df)
    
    return display()


@spark.route('/grid/<column>/tf_idf')
def tf_idf(column):
    df = datagrid.dataframe
    
    # Avoid loosing the original column
    orig_column = column
    column = column+'_temp'
    df = df.withColumn(column, df[orig_column])
    
    (df, voc) = ml.tf_idf(df, column)
    v = df.first()[column] # first vector
    
    for dim in range(0, v.size):
        dim_name = 'term_'+voc[dim] 
        
        # converts mllib.Vector --> numpy.array --> list
        # and extracts each dimension individually
        #vec.values.tolist()[dim], DoubleType()
        #vectorUDF = udf(lambda vec: vec[dim] if (vec and dim in vec.indices) else None, DoubleType())
        # vec.__dict__.__repr__()
        vectorUDF = udf(lambda vec: extract_sparse_vector(vec, dim), DoubleType())
        
        # coalesce if a column already exists.
        if dim_name in df.columns:
            df = df.withColumn(dim_name, 
                               coalesce(col(dim_name), vectorUDF(df[column])))
        else:
            df = df.withColumn(dim_name, vectorUDF(df[column]))
    
    df = df.drop(column)
    datagrid.setDataFrame(df)
    return display()


@spark.route('/grid/<column>/lda')
def lda(column):
    """
    Topic modeling using LDA.
    """
    df = datagrid.dataframe
    
    # Avoid loosing the original column
    orig_column = column
    column = column+'_temp'
    df = df.withColumn(column, df[orig_column])
    
    (df, topics, voc) = ml.lda(df, column)
    
    #'''
    for t in topics:
        #t = t.asDict()
        dim = t['topic']
        
        # uses term indices to retrieve the original term in vocabulary
        terms = [voc[i] for i in t['termIndices']]
        dim_name = 'topic__' + '_'.join(terms)
        
        # converts mllib.Vector --> numpy.array --> list
        # and extracts each dimension individually
        vectorUDF = udf(lambda vec: vec.values.tolist()[dim], DoubleType())
        df = df.withColumn(dim_name, vectorUDF(df[column]))
    #'''
    
    df = df.drop(column)
    datagrid.setDataFrame(df)
    return display()


@spark.route('/grid/<column>/label')
def label(column):
    """
    Create a new column containing labels based on another column.
    """
    df = datagrid.dataframe
    df = ml.label(df, column)
    datagrid.setDataFrame(df)
    return display()
    
