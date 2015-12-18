from pyspark import SparkContext, SparkConf

import sys, math


k = 10

def toRowKey(line):
    """
    Convert line of string into a key-value pair of the form 
    (rowno., (colno., element)).
    """
    elems = line.split()
    row = int(elems[0])
    col = int(elems[1])
    elem = float(elems[2])
    return (row, (col, elem))

def fill_row(row, x):
    """
    Add the element x to the row. row is a list and x is of the form
    (colno. element).
    """
    row.append(x)
    return row

def assemble_row(r1, r2):
    """
    Concatenate two pieces into one. Each ri is a list.
    """
    r1.extend(r2)
    return r1

def sort_row(row):
    """
    Sort elements in the given row according to the column number.
    row is of the form (rowno., [(colno., elem)]).
    """
    return (row[0], sorted(row[1], key=lambda pair: pair[0]))

def extract(users):
    """
    Extract k ratings from each user in the given users.
    users is of the form [(rowno., [(colno., rating)])]
    """
    test = []
    for u in users:
        test.append((u[0], u[1][0:k]))
    return test

def save_test_data(testdata):
    """
    Save test data to file.
    testdata is of the form [(rowno., [(colno., rating)])]
    """
    outname = 'data/testdata.txt'
    with open(outname, 'w') as out:
        for u in testdata:
            for p in u[1]:
                s = str(u[0]) + ' ' + str(p[0]) + ' ' + str(p[1]) + '\n'
                out.write(s)


#----------------------------------------------------------------

if __name__ == '__main__':
    conf = SparkConf()
    conf.setMaster('local[2]')
    conf.setAppName('ItemBased')
    conf.set('spark.executor.memory', '4g')

    sc = SparkContext(conf=conf)
    sourceFile = sys.argv[1] if len(sys.argv)>1 else 'data/sample1k.txt'

    rawdata = sc.textFile(sourceFile)
    users = rawdata.map(toRowKey) \
    			.aggregateByKey([], fill_row, assemble_row) \
    			.map(sort_row) \
                .sortBy(lambda x: len(x[1]), ascending=False)
    testdata = extract(users.take(25))
    
    save_test_data(testdata)
