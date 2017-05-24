# Databricks notebook source

import string
import json
import nltk

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer

nltk.download('stopwords')
nltk.download('punkt')

PUNCTUATION = set(string.punctuation)
STOPWORDS = set(stopwords.words('english'))
STEMMER = PorterStemmer()

def tokenize(text):
    tokens = word_tokenize(text)
    lowercased = [t.lower() for t in tokens]
    no_punctuation = []
    for word in lowercased:
        punct_removed = ''.join([letter for letter in word if not letter in PUNCTUATION])
        no_punctuation.append(punct_removed)
    no_stopwords = [w for w in no_punctuation if not w in STOPWORDS]
    stemmed = [STEMMER.stem(w) for w in no_stopwords]
    return [w for w in stemmed if w]

rdd = sc.textFile("/FileStore/tables/robj6l901488192665403/amber_01.txt")
result = rdd.flatMap(lambda x: [t[::-1] for t in tokenize(x) if t == t[::-1] and len(t) > 2]).countByValue()
for key, value in result.iteritems():
    print ("%s\t%i" % (key, value))



