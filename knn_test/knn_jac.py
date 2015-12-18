from pyspark import SparkContext, SparkConf
import sys, math

from utility import read_record, read_index, read_userID_file


def read_testdata(testname):
    testdata = dict()
    with open(testname, 'r') as testfile:
        for line in testfile:
            items = line.rstrip('\n').split()
            row = int(items[0])
            col = int(items[1])
            if row in testdata:
                testdata[row].append(col)
            else:
                testdata[row] = [col]
    return testdata

def read_recommend(recfile, userIndex, itemIndex):
    recdata = dict()
    with open(recfile, 'r') as rec:
        for line in rec:
            items = line.rstrip('\n').split()
            row = userIndex[items[0].rstrip(':')]
            cols = [itemIndex[i] for i in items[1:]]
            recdata[row] = cols
    return recdata

def avg_jac_sim(testdata, recdata):
    js = 0
    for u in testdata.keys():
        rec = recdata[u]
        test = testdata[u]
        js = js + jac_sim(rec, test)
    return js / len(testdata)

def jac_sim(v1, v2):
    s1 = set(v1)
    s2 = set(v2)
    return len(s1.intersection(s2)) / len(s1.union(s2))
    
#----------------------------------------------------------------

if __name__ == '__main__':
    testname = 'data/testdata.txt'
    userIndex_file = 'userIndex1m.txt'
    itemIndex_file = 'itemIndex1m.txt'
    recfile = 'output.txt'

    conf = SparkConf()
    conf.setMaster('local[2]')
    conf.setAppName('ItemBased')
    conf.set('spark.executor.memory', '4g')

    sc = SparkContext(conf=conf)

    testdata = read_testdata(testname)
    #print(testdata)

    userIndex = read_index(sc, userIndex_file, False)
    itemIndex = read_index(sc, itemIndex_file, False)
    recdata = read_recommend(recfile, userIndex, itemIndex)
    
    ajs = avg_jac_sim(testdata, recdata)
    print(ajs)

    #write_traindata(inname, outname, testdata)


 
