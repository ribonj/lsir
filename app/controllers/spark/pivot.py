from app import config
from app.controllers.spark import spark, sc, sqlContext
from pyspark.mllib.feature import Word2Vec
from flask import render_template, redirect, url_for, request, render_template
from os.path import join

import json


@spark.route('/pivot/')
def display_pivot():
    return render_template('pivot.html', table_names=sqlContext.tableNames())

@spark.route('/pivot/<table>')
def compute_pivot(table):
    return render_template('pivot.html', 
                           current_table=table,
                           table_names=sqlContext.tableNames())


@spark.route('/pivot/data/<table>')
def pivotData(table):
    query = 'SELECT * FROM ' + table
    data = sqlContext.sql(query).map(lambda r: r.asDict(True)).take(1000)
    return json.dumps(data)

def computePivot(table=None):
    
    if table is not None:
        query = 'SELECT * FROM ' + table
    else:
        query = ''
    
    df = None
    table = {'schema': {},
             'data': []}
    
    try:
        df = sqlContext.sql(query).groupBy("city").pivot("...") #TODO
        schema = df.schema.jsonValue()
        data = df.take(20)
        table = {'schema': schema,
                 'data': data}
        
    except:
        table = {'schema': {},
                 'data': []}
        
    return render_template('tables.html', 
                           query=query, 
                           table=table,
                           table_names=sqlContext.tableNames())

