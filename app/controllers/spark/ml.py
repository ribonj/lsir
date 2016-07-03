from app.controllers.spark import spark, sc, sqlContext

# SparkSQL
from pyspark.sql import Row
from pyspark.sql.functions import udf, when, col
from pyspark.sql.types import StructType, StructField, ArrayType, StringType

# ML - DataFrame
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, RegexTokenizer, StopWordsRemover, \
    HashingTF, IDF, Word2Vec, StringIndexer, CountVectorizer
from pyspark.ml.classification import LogisticRegression


# Natural Language Processing using NLTK
from nltk.stem import SnowballStemmer
stemmer = SnowballStemmer('english')


from pyspark.ml.clustering import LDA

def lda(df, column):
    df = preprocess(df, column) # text to list of terms
    (df, voc) = count(df, column) # add a feature column containing term counts
    
    # Trains the LDA model.
    # The input to LDA must be a dataframe containing a "features" column
    # (e.g. 10 topics and 100 iterations: k=10, maxIter=100)
    #lda = None
    lda = LDA(featuresCol=column, topicDistributionCol='_'+column, k=5, maxIter=20) 
    model = lda.fit(df)

    '''
    # compute likelihood and perplexity metrics
    ll = model.logLikelihood(df)
    lp = model.logPerplexity(df)
    print("The lower bound on the log likelihood: " + str(ll))
    print("The upper bound bound on perplexity: " + str(lp))
    #'''
    
    # Describe topics (using the 3 first terms)
    topics = model.describeTopics(3)
    #print("The topics described by their top-weighted terms:")
    #topics.show(truncate=False)

    # Shows the result
    df = model.transform(df)
    #df.show(truncate=False)
    df = replace(df, column, '_'+column)
    
    return (df, topics.collect(), voc)



def count(df, column):
    """
    Count the number of occurences of terms in documents.
    """
    # fit a CountVectorizerModel from the corpus.
    # vocabSize: top N words orderedby term frequency across the corpus
    # minDF: minimum number of documents a term must appear in to be 
    #   included in the vocabulary
    # e.g. vocabSize=10, minDF=2.0
    cv = CountVectorizer(inputCol=column, 
                         outputCol='_'+column)
    
    model = cv.fit(df)
    voc = model.vocabulary
    df = model.transform(df)
    
    df = replace(df, column, '_'+column)
    return (df, voc)


def label(df, column):
    """
    Create a labeled column.
    """
    indexer = StringIndexer(inputCol=column, outputCol=column+'_label')
    df = indexer.fit(df).transform(df)
    return df
    

def preprocess(df, column):
    """
    Choose the vocabulary to apply further machine learning algorithms.
    Pre-processing stages:
     - split text into terms/words
     - remove non-alphabetic terms (e.g. digits)
     - remove short terms (that are smaller than 3 characters)
     - remove common stop-words
     - apply stemming
    """
    df = tokenize(df, column)
    df = filterTokenAlpha(df, column)
    df = filterTokenLength(df, column)
    df = removeStopWords(df, column)
    df = stemming(df, column)
    return df


def replace(df, column, temp_column):
    """
    Replace the original column by the new, temporary column.
    """
    df = df.drop(column)
    df = df.withColumnRenamed(temp_column, column)
    return df


def tokenize(df, column):
    """
    Tokenize alpha-numeric words. Set all tokens to lower-case and 
    remove short terms having less than 3 characters.
    """
    # creates tokenizer based on regular expressions
    wordTokenizer = RegexTokenizer(
        inputCol=column, 
        outputCol='_'+column, 
        pattern='\w+'
    ).setGaps(False) # match tokens rather than gaps
    
    # transform: string --> array<string>
    df = wordTokenizer.transform(df) 
    
    df = replace(df, column, '_'+column)
    return df


def filterTokenAlpha(df, column):
    """
    Remove all non-alphabetic terms.
    Transformation: array<string> --> array<string>
    """
    # builds a user-defined function that removes terms containing 
    # any character other than letters
    alphaUDF = udf(
        lambda tokens: [t for t in tokens if t.isalpha()], 
        ArrayType(StringType()))
    
    # transform: array<string> --> array<string>
    df = df.withColumn(column, alphaUDF(df[column]))
    return df


def filterTokenLength(df, column, length=3):
    """
    Remove all non-alphabetic terms.
    Transformation: array<string> --> array<string>
    """
    # builds a user-defined function that removes terms having 
    # less than N character
    lengthUDF = udf(
        lambda tokens: [t for t in tokens if len(t) >= length], 
        ArrayType(StringType()))
    
    # transform: array<string> --> array<string>
    df = df.withColumn(column, lengthUDF(df[column]))
    return df


def removeStopWords(df, column):
    """
    Remove stop-words (like "the", "a", "I", etc.) from given column.
    The column must contain an array of strings.
    Transformation: array<string> --> array<string>
    """
    # creates remover to filter out common stop-words
    remover = StopWordsRemover(inputCol=column, outputCol='_'+column)
    
    # transform: array<string> --> array<string>
    df = remover.transform(df)
    
    df = replace(df, column, '_'+column)
    return df


def segmentSentences(df, column):
    """
    Split raw text into sentences. 
    This is useful to later perform Part-Of-Speech (POS) processing.
    """
    # builds a user-defined function that 
    # splits raw text into sentences
    sentUDF = udf(
        lambda text: nltk.sent_tokenize(text), 
        ArrayType(StringType()))
    
    # transform: string --> list<string>
    df = df.withColumn(column, sentUDF(df[column]))
    
    return df


def stemming(df, column):
    """
    Reduce terms/words to their root form. 
    e.g. is --> be, categorized --> category, etc.
    """
    # builds a user-defined function that stems words
    stemUDF = udf(
        lambda tokens: [stemmer.stem(t) for t in tokens], 
        ArrayType(StringType()))
    
    # transform: array<string> --> array<string>
    df = df.withColumn(column, stemUDF(df[column]))
    
    return df


def term_frequency(df, column):
    """
    Compute term-frequency of a token contained in a column.
    Transformation: array<string> --> vector
    """ 
    tf = HashingTF(inputCol=column, outputCol='_'+column)
    df = tf.transform(df)
    
    df = replace(df, column, '_'+column)
    return df


def tf_idf(df, column):
    """
    Compute TF-IDF of a corpus.
    Transformation: array<string> --> vector
    """ 
    df = preprocess(df, column) # text to list of terms
    (df, voc) = count(df, column)
    
    # creates a TF-IDF model and uses it to compute the feature vector.
    idf = IDF(inputCol=column, outputCol='_'+column)
    model = idf.fit(df)
    df = model.transform(df)
    
    df = replace(df, column, '_'+column)
    return (df, voc)


def word2vec(df, column):
    """
    Compute Word2Vec of a token contained in a column.
    The column must contain a list of string.
    """
    # Learn a mapping from words to Vectors.
    word2Vec = Word2Vec(vectorSize=2, 
                        minCount=0, 
                        inputCol=column, 
                        outputCol='_'+column)
    
    model = word2Vec.fit(df)
    df = model.transform(df)
    
    df = replace(df, column, '_'+column)
    return df


def chunk_named_entity(text):
    """
    Split the given raw text into sentences using a sentence segmenter;
    each sentence is further subdivided into words using a tokenizer;
    finally, each sentence is marked with part-of-speech tags.
    """
    import nltk
    
    # splits raw text into sentences: string --> list<string>
    sentences = nltk.sent_tokenize(text)
    
    # splits each sentence into a list of tokens: 
    # list<string> --> list<list<string>>
    sentences = [nltk.word_tokenize(s) for s in sentences]
    
    # tags every tokens contained in each sentence
    #list<list<string>> --> list<list<(string,string)>>
    sentences = [nltk.pos_tag(s) for s in sentences]
    
    # chunks sentences and extracts named entities (Entity Recognition)
    # list<list<(string,string)>> --> tree
    sentences = [nltk.ne_chunk(s) for s in sentences]
    
    return sentences.__repr__()


def entity_recognition(df, column):
    '''
    Possible variant for returned type:
    returned_type = ArrayType(ArrayType(StructType([
        StructField('token', StringType(), False),
        StructField('tag', StringType(), False)
    ])))
    '''
    returned_type = StringType()
    preprocessUDF = udf(chunk_named_entity, returned_type)
    df = df.withColumn(column, preprocessUDF(df[column]))
    return df

