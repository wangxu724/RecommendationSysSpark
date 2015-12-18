from pyspark import SparkContext, SparkConf

import sys, math


# number of similar items for each item
k = 100

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

def row_normalize(row):
    """
    Normalize the given row by subtracting the row average.
    row is of the form (rowno., [(colno., element)])
    """
    avg = sum([i[1] for i in row[1]]) / len(row[1])
    adjusted = [(i[0], i[1]-avg) for i in row[1]]
    return (row[0], adjusted)

def row_to_colKey(row):
    """
    Convert row to column key-value pari.
    row is of the form (rowno., [(colno., element)])
    Return value is of the form [(colno., (rowno., element))]
    """
    return [(i[0], (row[0], i[1])) for i in row[1]]

def fill_col(col, x):
    """
    Add the element x to the column. col is a list and x is of the form
    (rowno. element).
    """
    col.append(x)
    return col

def assemble_col(c1, c2):
    """
    Concatenate two pieces into one. Each ci is a list.
    """
    c1.extend(c2)
    return c1

def sort_col(col):
    """
    Sort elements in the given column according to the row number.
    col is of the form (colno., [(rowno., elem)]).
    """
    return (col[0], sorted(col[1], key=lambda pair: pair[0]))

def sort_col_by_sim(col):
    """
    Sort elements in the given column according to the similarity.
    col is of the form (colno., [(sim, colno.+)]).
    """
    sims = sorted(col[1], key=lambda pair: (pair[0], pair[1]), reverse=True)
    return (col[0], sims[0:k])
    

def find_norm(col):
    """
    Find the norm of the given col and append the norm to the col.
    col is of the form (colno., [(rowno., elem)]) before the function
    call and of the form (colno., ([(rowno., elem)], norm)) after.
    """
    norm = math.sqrt(sum([i[1]**2 for i in col[1]]))
    ret = [] if norm==0 else [(col[0], (col[1], norm))]
    return ret

def acos_sim(col_pair):
    """
    Find the adjusted-cosine similarity of the given pair of columns.
    col_pair is of the form (col1, col2), where coli is of the form
    (colno., ([(rowno., elem)], norm)).
    The return value is of the form ((colno.1, colno.2), cos_sim).
    """
    col1, col2 = col_pair
    c1 = col1[1][0]
    c2 = col2[1][0]
    dp = dot_prod(c1, c2)
    cs = dp / (col1[1][1] * col2[1][1])
    return ((col1[0], col2[0]), cs)

def dot_prod(v1, v2):
    """
    Find the dot product of the given two vectors.
    vi is of the form [(rowno., elem)].
    """
    dp = 0
    j = 0
    for i in range(len(v1)):
        while j<len(v2) and v1[i][0]>v2[j][0]:
            j = j + 1
        p = 0 if j>=len(v2) or v2[j][0]>v1[i][0] else v2[j][1]*v1[i][1]
        dp = dp + p
    return dp

def extract_sim(sim_pair):
    """
    Convert similarity pair of the form ((colno.1, colno.2), sim) into 
    the new form [(colno.1, (sim, colno.2)), (colno.2, (sim, colno.1))].
    """
    cn1 = sim_pair[0][0]
    cn2 = sim_pair[0][1]
    sim = sim_pair[1]
    return [(cn1, (sim, cn2)), (cn2, (sim, cn1))]

#----------------------------------------------------------------

if __name__ == '__main__':
    conf = SparkConf()
    conf.setMaster('local[2]')
    conf.setAppName('ItemBased')
    conf.set('spark.executor.memory', '4g')

    sc = SparkContext(conf=conf)
    sourceFile = sys.argv[1] if len(sys.argv)>1 else 'data/sample1k.txt'

    rawdata = sc.textFile(sourceFile)
    itemdata = rawdata.map(toRowKey) \
                .aggregateByKey([], fill_row, assemble_row) \
                .map(row_normalize) \
                .flatMap(row_to_colKey) \
    			.aggregateByKey([], fill_col, assemble_col) \
    			.map(sort_col).flatMap(find_norm)
    itempair = itemdata.cartesian(itemdata).filter(lambda (i1, i2): i1[0]<i2[0])
    acos_sim_pair = itempair.map(acos_sim)
    sim_data = acos_sim_pair.flatMap(extract_sim) \
               .aggregateByKey([], fill_col, assemble_col) \
               .map(sort_col_by_sim)
    sim_data.saveAsPickleFile('acos_sim')