from pyspark import SparkContext
from pyspark import SparkConf
from sets import Set

import json





def read_json(sc, filename):
    #read the json file return a RDD of (reviewerID, [(asin, overall),...])
    rawdata = sc.textFile(filename)
    rawdata = rawdata.map(json.loads).map(lambda a: (a['reviewerID'], Set([(a['asin'], a['overall'])])))
    vectors = rawdata.reduceByKey(lambda a, b : a.union(b))
    vectors = vectors.map(lambda (k, v): (k, sorted(list(v))))
    return vectors 


def read_txt(sc, filename):
    #read the txt file return a RDD of (reviewerID, [(asin, overall),...])
    rawdata = sc.textFile(filename)
    rawdata = rawdata.map(lambda t : tuple(t.split(',')))\
            .filter(lambda t: len(t) == 3)\
            .map(lambda (a,b,c): (a,Set([(b,float(c))])))
    vectors = rawdata.reduceByKey(lambda a, b : a.union(b))
    vectors = vectors.map(lambda (k, v): (k, sorted(list(v))))
    return vectors 





















