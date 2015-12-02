from pyspark import SparkContext
from pyspark import SparkConf





import sys
import json











if __name__ == '__main__':
    if len(sys.argv) != 3:
        print "Usage: spark-submit dataExtracter.py <input_file> <output_file>"
        sys.exit(0)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

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

    rawData = rawData.map(json.loads).map(lambda a: (a['reviewerID'], a['asin'], a['overall']))

    data = rawData.collect()

    f = open(output_file,'w')
    
    for d in data:
        f.write("%s,%s,%f\n" % (d[0], d[1], d[2]))

    f.close()


