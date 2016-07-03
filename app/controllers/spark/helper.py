from nltk.corpus import wordnet

def get_wordnet_pos(treebank_tag):
    
    # ADJ, ADJ_SAT, ADV, NOUN, VERB = 'a', 's', 'r', 'n', 'v'
    # POS_LIST = [NOUN, VERB, ADJ, ADV]
    if treebank_tag.startswith('J'):
        return wordnet.ADJ
    elif treebank_tag.startswith('V'):
        return wordnet.VERB
    elif treebank_tag.startswith('N'):
        return wordnet.NOUN
    elif treebank_tag.startswith('R'):
        return wordnet.ADV
    else:
        return ''
    
    
def extract_sparse_vector(vector, index):
    # zip the indices with the values
    v = vector.__dict__
    v = dict(zip(v['indices'], v['values']))
    
    if index in v.keys():
        return float(v[index])
    else:
        return 0.0