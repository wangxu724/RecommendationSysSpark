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

def evaluate(testdata, sim):
    """
    Uses the testdata and similarity to test item based recommendation.
    testdata is of the form [(rowno., [(colno., rating)])]
    sim is of the form [(colno., [(sim, colno.+)])]
    """
    total_dist = 0
    for test_user in testdata:
        # of the form [(colno., rating)]
        removed = test_user[1][0:k]
        remain = test_user[1][k:]
        user = dict(remain)
        recommended = sim.flatMap(lambda x: recommend(x, user)) \
                        .reduceByKey(lambda p1, p2: (p1[0]+p2[0], p1[1]+p2[1])) \
                        .map(normalize) \
                        .sortBy(lambda p: p[1], ascending=False).take(k)
        dist = math.sqrt(vec_dist(recommended, removed))
        total_dist = total_dist + dist
    return total_dist / len(testdata)

def normalize(p):
    """
    Normalize the rating.
    """
    r = 0 if p[1][1]==0 else p[1][0]/p[1][1]
    return (p[0], r)
        
def recommend(sim, user):
    """
    Recommend items to given user via item based recommendation.
    sim is of the form (colno., [(sim, colno.+)])
    user is of the form dict[colno.] = rating
    """
    rec = []
    if sim[0] in user:
        r = user[sim[0]]
        rec.extend([(p[1], (p[0]*r, p[0])) for p in sim[1]])
    return rec

def vec_dist(v1, v2):
    """
    Find the distance between the given two vectors.
    vi is of the form [(colno., elem)].
    """
    dist = 0
    j = 0
    for i in range(len(v1)):
        while j<len(v2) and v1[i][0]>v2[j][0]:
            dist = dist + v2[j][1]**2
            j = j + 1
        p = v1[i][1]**2 if j>=len(v2) or v2[j][0]>v1[i][0] \
                            else (v2[j][1]-v1[i][1])**2
        dist = dist + p
    if len(v1)==0:
        dist = sum(v[1]**2 for v in v2)
    return dist

#----------------------------------------------------------------

if __name__ == '__main__':
    conf = SparkConf()
    conf.setMaster('local[2]')
    conf.setAppName('ItemBased')
    conf.set('spark.executor.memory', '4g')

    sc = SparkContext(conf=conf)
    sourceFile = sys.argv[1] if len(sys.argv)>1 else 'data/sample1k.txt'
    similarity = sys.argv[2] if len(sys.argv)>1 else 'cos_sim'

    rawdata = sc.textFile(sourceFile)
    users = rawdata.map(toRowKey) \
    			.aggregateByKey([], fill_row, assemble_row) \
    			.map(sort_row) \
                .sortBy(lambda x: len(x[1]), ascending=False)
    testdata = users.take(10)
    
    sim = sc.pickleFile(similarity)
    error = evaluate(testdata, sim)
    print(error)
