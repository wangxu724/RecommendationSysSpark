#Recommend using k nearest neighbors
from pyspark import SparkContext
from pyspark import SparkConf
from sets import Set

import sys
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





def getUserVector(user_id, vectors):
    user = vectors.filter(lambda a: a[0] == user_id).collect()
    if len(user) == 0:
        return None
    else:
        return user[0]

def mergeArray(a,b,G,rev):
    r = sorted(a+b, key=lambda a: a[1], reverse=rev)
    return r[0:G]

def listhelper(a):
    r = []
    r.append(a)
    return r


def getDistance1(v1, v2):
    rlt = 0.0
    i = 0
    j = 0
    while i < len(v1) and j < len(v2):
        if v1[i][0] == v2[j][0]:
            t = v1[i][1] - v2[j][1]
            if t < 0:
                t = 0 - t
            rlt = rlt + t
            i = i + 1
            j = j + 1
        elif v1[i][0] < v2[j][0]:
            t = v1[i][1]
            if t < 0:
                t = 0 - t
            rlt = rlt + t
            i = i + 1
        elif v1[i][0] > v2[j][0]:
            t = v2[j][1]
            if t < 0:
                t = 0 - t
            rlt = rlt + t
            j = j + 1
    
    while i < len(v1):
        t = v1[i][1]
        if t < 0:
            t = 0 - t
        rlt = rlt + t
        i = i + 1

    while j < len(v2):
        t = v2[j][1]
        if t < 0:
            t = 0 - t
        rlt = rlt + t
        j = j + 1

    return rlt


def knn1(sc, v, vectors, k):
    #v is a (reviewerID, [(asin, overall)]), vectors is a RDD of (reviewerID, [(asin, overall)])
    #return a RDD of (reviewerID, [(asin, overall)])
    G = 200
    if vectors.count() < k:
        return vectors
    rlt = vectors.map(lambda a: (a[0], getDistance1(a[1], v[1]), a[1])).zipWithIndex()\
            .map(lambda (t, k): (k/G, listhelper(t)))\
            .reduceByKey(lambda a,b: a+b)\
            .map(lambda (k,t): t)\
            .reduce(lambda a,b: mergeArray(a,b,G, False))

    return sc.parallelize(rlt[0:100])








def getSimilarity2(v1, v2):
    h = {}
    for t in v1:
        if not t[0] in h:
            h[t[0]] = [t[1]]
        else:
            h[t[0]] = h[t[0]].append(t[1])

    for t in v2:
        if not t[0] in h:
            h[t[0]] = [t[1]]
        else:
            h[t[0]] = h[t[0]].append(t[1])

    sim = 0.0
    for v in h:
        if len(v) == 2:
            d = v[0] - v[1]
            if d < 0:
                d = 0 - d
            sim = sim + 5.0 - d
    return sim/float(len(h))


def knn2(sc, v, vectors, k):
    #v is a (reviewerID, [(asin, overall)]), vectors is a RDD of (reviewerID, [(asin, overall)])
    #return a RDD of (reviewerID, [(asin, overall)])
    G = 200
    if vectors.count() < k:
        return vectors
    rlt = vectors.map(lambda a: (a[0], getSimilarity2(a[1], v[1]), a[1])).zipWithIndex()\
            .map(lambda (t, k): (k/G, listhelper(t)))\
            .reduceByKey(lambda a,b: a+b)\
            .map(lambda (k,t): t)\
            .reduce(lambda a,b: mergeArray(a,b,G, True))

    return sc.parallelize(rlt[0:100])















def getRecommend(neighbors):
    item = neighbors.flatMap(lambda a: [ (i[0], 1) for i in a[2]])
    rlt = item.reduceByKey(lambda a, b: a+b).sortBy(lambda a: a[1], ascending=False).take(5)
    return rlt


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print "Usage: spark-submit knn.py user_id data_file"
        sys.exit(0)

    user_id = sys.argv[1]
    data_file = sys.argv[2]

    conf = SparkConf()
    conf.setMaster("local[8]")
    conf.setAppName("matmult")
    conf.set("spark.executor.memory", "16g")
    conf.set("spark.driver.memory", "16g")
    conf.set("spark.python.worker.memory", "16g")
    conf.set("spark.storage.memoryFraction", "0.8")
    conf.set("spark.shuffle.memoryFraction", "0.8")
    conf.set("spark.shuffle.manager", "sort")
    sc = SparkContext(conf=conf)

    #vectors = read_json(sc, data_file)
    vectors = read_txt(sc, data_file)

    user_vector = getUserVector(user_id, vectors)

    K = 100

    neighbors = knn2(sc, user_vector, vectors, K) 

    r = getRecommend(neighbors) 

    print '*************'
    print r



