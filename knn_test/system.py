
#Recommend using k nearest neighbors(using product as user's feature)
from pyspark import SparkContext
from pyspark import SparkConf
from sets import Set
from utility import read_record, read_index, read_userID_file
from knn import getUserVector, knn1, knn2, getRecommend

import sys
import json








if __name__ == '__main__':
    if len(sys.argv) != 5:
        print "Usage: spark-submit system.py userID_file record_file userIndex_file itemIndex_file"
        sys.exit(0)

    userID_file = sys.argv[1]
    record_file = sys.argv[2]
    userIndex_file = sys.argv[3]
    itemIndex_file = sys.argv[4]
    K = 100

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

    vectors = read_record(sc, record_file)
    itemIndex = read_index(sc, itemIndex_file, True)
    userIndex = read_index(sc, userIndex_file, False)
    user_id = read_userID_file(sc, userID_file)


    f = open('output.txt','w')

    for u in user_id:
        uid = userIndex[u]
    
        user = getUserVector(uid, vectors)
    
        otherUser = vectors.filter(lambda (k,v): k != uid)
        neighbors = knn2(sc, user, otherUser, K) 
        #neighbors = knn1(sc, user, otherUser, K) 

        rlt = getRecommend(neighbors) 
        
        tmp = ""
        for r in rlt:
            if r[0] in itemIndex:
                tmp = tmp + itemIndex[r[0]] + ' '

        f.write("%s: %s\n" % (u, tmp))

    f.close()






