from pyspark import SparkContext
from pyspark import SparkConf





import sys
import json











if __name__ == '__main__':
    if len(sys.argv) != 5:
        print "Usage: spark-submit dataExtracter.py <input_file> <output_file> <userIndex_file> <itemIndex_file>"
        sys.exit(0)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    userIndex_file = sys.argv[3]
    itemIndex_file = sys.argv[4]

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

    rawData = sc.textFile(input_file)
    rawData = rawData.map(json.loads)\
            .map(lambda a: (a['reviewerID'], a['asin'], a['overall']))

    userIndex = rawData.map(lambda (a,b,c): a)\
            .distinct()\
            .zipWithIndex()\
            .collectAsMap()

    itemIndex = rawData.map(lambda (a,b,c): b)\
            .distinct()\
            .zipWithIndex()\
            .collectAsMap()



    #generate userIndex_file
    f = open(userIndex_file, 'w')
    for k in userIndex.keys():
        f.write("%s %d\n" % (k, userIndex[k]))
    f.close()


    #generate itemIndex_file
    f = open(itemIndex_file, 'w')
    for k in itemIndex.keys():
        f.write("%s %d\n" % (k, itemIndex[k]))
    f.close()


    #generate output_file
    data = rawData.map(lambda (a,b,c): (userIndex[a], itemIndex[b],c))\
            .collect()
    f = open(output_file,'w')
    for d in data:
        f.write("%d %d %f\n" % (d[0], d[1], d[2]))
    f.close()


