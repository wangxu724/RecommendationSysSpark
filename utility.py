from pyspark import SparkContext
from pyspark import SparkConf
from sets import Set

import json






def read_record(sc, filename):
    #read the txt file return a RDD of (reviewerID, [(asin, overall),...])
    rawdata = sc.textFile(filename)
    rawdata = rawdata.map(lambda t : tuple(t.split(',')))\
            .filter(lambda t: len(t) == 3)\
            .map(lambda (a,b,c): (int(a),Set([(int(b),float(c))])))
    vectors = rawdata.reduceByKey(lambda a, b : a.union(b))
    vectors = vectors.map(lambda (k, v): (k, sorted(list(v))))
    return vectors 


def read_index(sc, filename, reverse):
    #read the indexfile return a hash of {index->name}
    index = sc.textFile(filename)\
            .map(lambda t: tuple(t.split(',')))
    if reverse:
        index = index.map(lambda (a,b): (int(b),a))
    else:
        index = index.map(lambda (a,b): (a,int(b)))
    return index.collectAsMap()


















