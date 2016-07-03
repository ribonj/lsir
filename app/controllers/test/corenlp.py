from app import config
from app.controllers.test import test

from pycorenlp import StanfordCoreNLP
nlp = StanfordCoreNLP(config.CORENLP_URL)

@test.route("/corenlp")
def corenlp():
    text = 'hello world world'
    
    output = nlp.annotate(text, properties={
        'annotators': 'tokenize,ssplit,pos,depparse,parse',
        'outputFormat': 'json'})
    
    print(output['sentences'][0]['parse'])
                          
    return 'CoreNLP ok!'

