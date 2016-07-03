from app.controllers.test import test

import flask
from datetime import datetime

from elasticsearch import Elasticsearch
es = Elasticsearch()

@test.route("/elastic_index")
def elastic_index():
    doc = {
        'author': 'kimchy',
        'text': 'Elasticsearch: cool. bonsai cool.',
        'timestamp': datetime.now(),
    }
    res = es.index(index="test-index", doc_type='tweet', body=doc)
    print(res['created'])
    return res.__str__()
    
@test.route("/elastic_get")
def elastic_get():
    res = es.get(index="test-index", doc_type='tweet', id=1)
    print(res['_source'])
    return res.__str__()
    
@test.route("/elastic_search")
def elastic_search():
    es.indices.refresh(index="test-index")
    res = es.search(index="test-index", body={"query": {"match_all": {}}})
    print("Got %d Hits:" % res['hits']['total'])
    
    for hit in res['hits']['hits']:
        print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])
        
    return res.__str__()


@test.route("/elastic_analyze/<text>")
def elastic_analyze(text):
    result = es.indices.analyze(analyzer="english", text=text)
    return flask.jsonify(**result)

