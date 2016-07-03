from app import config, sparkContext, sqlContext, Row
from app.controllers.test import test

from flask import request, url_for, render_template
from werkzeug import secure_filename

import math


@test.route('/wordcount/<file>')
def countWords(file):
    input = config.HDFS_USER_URL + "/input/" + file # mapred-queues.xml.template
    output = config.HDFS_USER_URL + "/output"

    data = sparkContext.textFile(input).cache() # cache is optional
    
    words = data.flatMap(lambda line: line.split(' '))
    
    wordCounts = words.map(lambda word: (word, 1)) \
        .reduceByKey(lambda c1,c2: c1+c2) \
    
    result = ""
    for (word, count) in wordCounts.collect():
        result += "(%s: %i)<br/>" % (word, count) 
    
    return result
    
    
@test.route('/sql')
def testSparkSql():
    # Create a DataFrame of Customer objects from the dataset text file.
    # note: there is a library from databrick
    
    # load text file as an RDD of line
    #linesRDD = sparkContext.textFile('data/users.csv')
    linesRDD = sparkContext.textFile(config.HDFS_DATA_URL + '/users.csv')
    
    # transforms each line into a tuple
    # note: do not use flatMap to keep columns grouped by line
    tuplesRDD = linesRDD.map(lambda line: line.split(','))
    
    # converts each tuple into a dictionary: deprecated!
    '''
    usersRDD = tuplesRDD.map(lambda tuple: {"id": int(tuple[0]), \
                                            "name": tuple[1], \
                                            "place": tuple[2], \
                                            "trx": tuple[3], \
                                            "code": int(tuple[4])})
    '''
    
    # Variant: create the DataFrame using the Row object
    usersRDD = tuplesRDD.map(lambda tuple: Row(id=int(tuple[0]), \
                                               name=tuple[1], \
                                               place=tuple[2], \
                                               transaction=tuple[3], \
                                               code=int(tuple[4])))
    
    # creates a DataFrame by infering the schema: deprecated!
    '''usersDF = sqlContext.inferSchema(usersRDD)'''
    
    # Variant: create the DataFrame based on an RDD of Rows
    usersDF = sqlContext.createDataFrame(usersRDD)
    
    # Infer schema and register as table
    usersDF.registerTempTable("users")
    
    # Displays content of dataframe in command line
    usersDF.show()
    
    # Display schema in command line
    usersDF.printSchema()
    
    # Select and filter result using SqlContext or DataFrame
    sqlContext.sql('SELECT id, name FROM users where id < 400').show()
    #df.select("id", "name").filter("id < 400").show() # no need for temp table
    
    return 'ok'


@test.route("/json")
def json():
    #input = sqlContext.jsonFile('data/users.json')
    input = sqlContext.jsonFile(config.HDFS_DATA_URL + '/users.json')
    input.printSchema()
    input.show()
    return "JSON: OK!"


@test.route("/parquet")
def parquet():
    
    data = sparkContext.parallelize([
         Row(id=1, name='juls', email='jr@airel.ch'),
         Row(id=2, name='ste', email='sr@airel.ch')])
    
    usersDF = sqlContext.createDataFrame(data)
    
    #usersDF.write.parquet("data/users.parquet")
    #usersDF = sqlContext.read.parquet('data/users.parquet')
    usersDF = sqlContext.read.parquet(config.HDFS_DATA_URL + '/users.parquet')
    usersDF.registerTempTable("users")
    filteredUsers = sqlContext.sql("select name from users where id=1")
    
    # read text file and con
    #input = sqlContext.jsonFile('data/users.json')
    usersDF.printSchema()
    filteredUsers.show()
    
    return "Parquet: OK!"

# compute cosine similarity between two given vector a and b:
# similarity = 1 - distance = 1 - (a dot b) / (||a||*||b||)
def consine_similarity(a,b):
    sumaa = 0
    sumbb = 0
    sumab = 0
    
    for i in range(len(a)):
        ai = a[i]
        bi = b[i]
        sumaa += ai*ai
        sumbb += bi*bi
        sumab += ai*bi
        
    similarity = sumab/math.sqrt(sumaa*sumbb)
    similarity = int(similarity * 100) / 100.0 # round by 2 decimal
    return similarity


@test.route("/cossim")
def cossim():
    a = [1,2,3,4]
    b = [0,0,9,1]
    sim = consine_similarity(a,b)
    result = {"consine_similarity": sim}
    return result.__repr__()


def inverse(x):
    doc_path = x[0]
    
    # split a string at position of a given substring,
    # starting to search from the end of input string.
    # this is equivalent to x.rsplit('-', 1)[1].
    doc_name = doc_path.rpartition('/')[2]
    
    word_list = []
    
    for word in x[1].split(' '):
        norm_word = word.strip().lower()
        word_list.append((norm_word, set([doc_name])))
        
    return word_list


def vectorize(index_item, words):
    doc_id = index_item[0]
    word_list = index_item[1]
    vector = []
    
    for w in words:
        if w in word_list:
            vector.append(1)
        else:
            vector.append(0)
            
    return (doc_id, vector)


@test.route("/indexing")
def indexing():
    
    input = sparkContext.wholeTextFiles(config.UPLOAD_FOLDER) # RDD of lines
    
    index = input.map(lambda x: (
        #int(x[0][-5:-4]), 
        x[0].rpartition('/')[2],
        sorted(list(set(x[1].split(' ')))) )).sortByKey()
    
    inverted_index = input.flatMap(inverse).reduceByKey(lambda d1, d2: d1|d2)\
        .map(lambda x: (x[0], sorted(list(x[1])))).sortByKey()
    
    # local dictionary (small enough)
    # TODO: use shared variables!!
    words = inverted_index.keys().collect()
    docs = index.keys().collect()
    doc_count = index.count()
    
    doc_vectors = index.map(lambda x: vectorize(x, words))
    
    doc_similarities = doc_vectors.cartesian(doc_vectors)
    #doc_similarities = doc_similarities.filter(lambda x: x[0][0] < x[1][0])
    doc_similarities = doc_similarities.map(lambda x: (
        (x[0][0],x[1][0]), 
        consine_similarity(x[0][1],x[1][1]) ))
    
    matrix = doc_similarities.collect()
    
    # example: 
    #  [((u'doc1.txt', u'doc2.txt'), 0.35), 
    #   ((u'doc1.txt', u'doc3.txt'), 0.0), 
    #   ((u'doc2.txt', u'doc3.txt'), 0.5)]
    #
    # build (NxN)-matrix locally
    # TODO: optimize with spark!
    
    # Build table dictionary for template table.html
    table = {
        "name": "Documents",
        "rows": docs,
        "cols": docs,
        "data": [ [0]*doc_count for i in range(doc_count)]
    }
    
    # Fill in data in table
    for ((d1,d2),s) in matrix:
        row = docs.index(d1)
        col = docs.index(d2)
        table['data'][row][col] = s
    
    return render_template("table.html", table=table)


@test.route("/index_test")
def matrix():
    count = 3
    names = ["doc1", "doc2", "doc3"]
    table = {"name": "Table",
             "rows": names,
             "cols": names,
             "data": [ [0]*count for i in range(count)] }
    table['data'][0][1] = 1
    return render_template("table.html", table=table)


# check if extension of current file is allowed
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in config.ALLOWED_EXTENSIONS
    
    
#@test.route('/uploads/<filename>')
#def uploaded_file(filename):
#    return send_from_directory(config.UPLOAD_FOLDER, filename)



