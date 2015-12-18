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
            rat = float(items[2])
            testdata[(row, col)] = rat
    return testdata

def write_traindata(inname, outname, testdata):
    with open(outname, 'w') as outfile:
        with open(inname, 'r') as infile:
            for line in infile:
                items = line.rstrip('\n').split()
                row = int(items[0])
                col = int(items[1])
                if (row, col) not in testdata:
                    outfile.write(line)

def write_test_id(testdata, userIndex):
    outname = 'data/train/userIDs1.txt'
    ids = set([k[0] for k in testdata.keys()])
    with open(outname, 'w') as out:
        for id in ids:
            out.write(userIndex[id] + '\n')


    
    
#----------------------------------------------------------------

if __name__ == '__main__':
    testname = 'data/testdata.txt'
    inname = 'data/sample1m.txt'
    outname = 'data/train/sample1m.txt'
    userIndex_file = 'userIndex1m.txt'

    conf = SparkConf()
    conf.setMaster('local[2]')
    conf.setAppName('ItemBased')
    conf.set('spark.executor.memory', '4g')

    sc = SparkContext(conf=conf)

    testdata = read_testdata(testname)
    userIndex = read_index(sc, userIndex_file, True)

    write_test_id(testdata, userIndex)

    #write_traindata(inname, outname, testdata)


 
