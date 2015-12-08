import re
import sys
import os
import cmath
import time
from pyspark import SparkContext, SparkConf

'''
Data format with uid, iid, rating should work 
It save the result in 'item_based_[DATA_ALL]'
'''

DATADIR = "data/"


# 1k, 1m, 2m
SIZE_RECORD = ["1k", "5k", "1m", "2m", "10m", "20m", "50m", "100m", "all"]
USE = 1
DATA_All = "sample"+SIZE_RECORD[USE]+".txt"
DATA_ITEM = "itemIndex"+SIZE_RECORD[USE]+".txt"
DATA_USER = "userIndex"+SIZE_RECORD[USE]+".txt"

def print_out(x):
    print "print_out"
    print x
    return x;

def print_out1(x, x1):
    print "print_out1"
    print x
    if len(x1)>1:
        print x1
    return (x, x1);


'''
key = item_id
value = [<uid, rating>, ... ]

find rating's count for each item at each rate
return (key, [rating_counts])
'''
def intoItemRatingCounts(key, values):  
    # print (key, values) 
    item_rating = [0,0,0,0,0]
    for rate in values :
        item_rating[rate[1]-1] += 1
    return(key, item_rating)

def intoItemUsrRate(x):
    x = x.split(' ')      # user_id, item_id, rating
    return (x[1], (x[0], int(float(x[2])) ) )

'''

'''
def intoUsrItemRateCount(x):
    x = x.split(' ')
    # iid = x[0];
    # uid = x[1];
    # item_rating = x[2];
    return (x[1], x[0], x[2])

def pairCount(id, count):
    x = x.split(' ')
    return (x[1], count)

def intoUsrItemRateCount2(user_item_rating, item_rating_count):
    # print item_rating_count.take(1)
    user_item_rating_count = user_item_rating.map(lambda x : pair(x, item_rating_count))
    return user_item_rating_count;

def intoUsrItemRatingCount(x):
    # iid, <uid, rating>, rating_count
    # print (x, (x[1][0][0], (x[0], x[1][0][1], x[1][1][ x[1][0][1]-1 ])))
    return(x[1][0][0], (x[0], x[1][0][1], x[1][1][ x[1][0][1]-1 ]))
    # uid, iid, rating, rating_count

'''
key = <item>
value = <uid, rating>>
return [item, <uid, rating, rating_count>]
'''
def phase1(usr_item_rating):
    print "phase1" 
    #maper
    #[ item, <uid, rating> ]
    item_usr_rating = usr_item_rating.map(lambda (x): intoItemUsrRate(x) ).sortByKey();
    # item_usr_rating.map(lambda x: print_out(x));
    #reducer
    #[item, [<rating,totalCount>, ...], rating_Count]
    item_rating_count = item_usr_rating.groupByKey().map(lambda x : intoItemRatingCounts(x[0], list(x[1])))
    #maper
    #[item, <uid, rating, rating_count> ]
    usr_item_rating_count = item_usr_rating.leftOuterJoin(item_rating_count).map(lambda x: intoUsrItemRatingCount(x)) #.map(lambda x: print_out(x))
    # print item_rating_count.take(1)
    # ITEM_RATING_COUNT = item_rating_count;
    # user_item_rating_count = intoUsrItemRateCount2(user_item_rating, item_rating_count);
    # user_item_rating_count = user_item_rating.map(lambda x : intoUsrItemRateCount(x))
    #[<item,rating>, totalCount at rating]
    #[uid, <item, rating, totalCount at rating>]
    #return <uid, item, rating, rating_count> 
    return usr_item_rating_count


# item_rating = phase1(temp)
# item_rating.take(10)

# group = phase2(movie_rating)
# group.take(20)

def pairItems(key, values):
    pairs = []
    # print(key)
    # print(values)
    # pairs = {};
    # print len(values)
    for v in range(0,len(values)):
        key = str(int(values[v][0]))
        # print "key1: "+key;
        for v1 in range(v+1,len(values)):
            key=""
            if values[v][0]<values[v1][0] :
                key = str(int(values[v][0])) + ", " +str(int(values[v1][0]))
                pairs.append((key,(values[v][1], values[v][2], values[v1][1], values[v1][2], values[v][1]*values[v1][1], values[v][1]*values[v][1], values[v1][1]*values[v1][1])))
                # pairs[key] = (values[v][1], values[v][2], values[v1][1], values[v1][2], values[v][1]*values[v1][1], values[v][1]*values[v][1], values[v1][1]*values[v1][1])
                # print ("key: "+key + " -- ", values[v], values[v1])
            else:
                key = str(int(values[v1][0])) + ", " +str(int(values[v][0]))
                pairs.append((key,(values[v1][1], values[v1][2], values[v][1], values[v][2], values[v1][1]*values[v][1], values[v1][1]*values[v1][1], values[v][1]*values[v][1])))
                # pairs[key] = (values[v1][1], values[v1][2], values[v][1], values[v][2], values[v1][1]*values[v][1], values[v1][1]*values[v1][1], values[v][1]*values[v][1])
                # print ("key: "+key + " -- ", values[v1], values[v])
            # print values[v] 
            # print values[v1]
    return pairs

def intoRDD(list_pairs):
    items = [];
    list_pairs = list_pairs.collect()
    # print list_pairs
    for pairs in list_pairs:
        for p in pairs:
            if p:
                items.append(p)
    return sc.parallelize(items)

'''
key = <uid>
value = <it, it_rating, it_rating_count>
return [<it1, it2>, <rating_it1, r_it1_count, r_it2, r_it2_count, r_it1*r_it2, r_it1^2, r_it2^2>]
'''
def phase2(usr_item_rating_count):
    print "phase2"
    #recucer
    #[ (uid, [<item, rating, rating_count>, ... ]) , ... ]
    # usr_items_rating_count = usr_item_rating_count.groupByKey().map(lambda x: print_out(x)).map(lambda x: pairItems(x[0], list(x[1])))
    usr_items_rating_count = usr_item_rating_count.groupByKey().map(lambda x: pairItems(x[0], list(x[1])))
    return usr_items_rating_count

def calculateJaccard(total_rating_groups, r_it1_count_max, r_it2_count_max):
    divider = float(r_it1_count_max+r_it2_count_max-total_rating_groups)
    if divider<=0:
        return 'nan';
    return (float(total_rating_groups)/divider)

def calculateCosine(dot_prod, r_it1_sqrt, r_it2_sqrt):
    divider = float(r_it1_sqrt*r_it2_sqrt)
    if divider<=0:
        return 'nan';
    return (float(dot_prod)/ divider)

def calculatePearson(total_rating_groups, dot_prod, r_it1_sum, r_it2_sum, r_it1_sqrt, r_it2_sqrt):
    left = total_rating_groups * r_it1_sqrt - r_it1_sum*r_it1_sum
    right = total_rating_groups * r_it2_sqrt - r_it2_sum*r_it2_sum
    divider = (cmath.sqrt(left) * cmath.sqrt(right))
    divider = str(divider).replace('(','').replace(')','').replace('+0j','').replace('j','')
    divider = float(divider)
    # if left>0 and right>0:
    #     divider = (math.sqrt(left) * math.sqrt(right))
    # el
    if divider<=0:
        return 'nan';
    return ((total_rating_groups*dot_prod - r_it1_sum*r_it2_sum)/divider)

#<rating_it1, r_it1_count, r_it2, r_it2_count, r_it1*r_it2, r_it1^2, r_it2^2>
def findSimilarities(key, values): 
    size = len(values)
    dot_prod = 0
    rating_it1_sum = 0
    rating_it1_Normal = 0
    rating_it1_max_count = 0
    rating_it2_sum = 0
    rating_it2_Normal = 0
    rating_it2_max_count = 0
    for v in values:
        dot_prod += v[4]
        rating_it1_sum += v[0]
        rating_it2_sum = v[2]
        if v[1]>rating_it1_max_count:
            rating_it1_max_count = v[1]
        if v[3]>rating_it2_max_count:
            rating_it2_max_count = v[3]
        rating_it1_Normal += v[5]
        rating_it2_Normal += v[6]
    pearson = calculatePearson(size, dot_prod, rating_it1_sum, rating_it2_sum, rating_it1_Normal, rating_it2_Normal)
    jaccard = calculateJaccard(size, rating_it1_max_count, rating_it2_max_count)
    cosine = calculateCosine(dot_prod, rating_it1_Normal, rating_it2_Normal)
    return(key, (pearson, cosine, jaccard))



'''
key = <item1, item2>
value = <rating_it1, r_it1_count, r_it2, r_it2_count, r_it1*r_it2, r_it1^2, r_it2^2]>
return [<it1, it2>, <pearson, jaccard, cosine>]
'''
def phase3(pairs):
    print "phase3" 
    #map
    #[(<item1, item2>, [ <rating1, rating1 count, rating2, rating2 count, rating1*rating2, rating1^2, rating2^3>, ... ] ), ... ]
    items_rating_count = intoRDD(pairs).groupByKey() #.map(lambda x: print_out1(x[0], list(x[1])))
    #reducer
    #[<it1, it2>, <pearson, cosine, jaccard>]
    result = items_rating_count.map(lambda x : findSimilarities(x[0], list(x[1])))
    return result

# def exportToFile(filename, data):

def outPutTime(start_time, end_time):
    m, s = divmod(end_time-start_time, 60)
    h, m = divmod(m, 60)
    print "Total Time: "
    print "%d:%02d:%02d" % (h, m, s)

def main():
    DATA_All = "sample"+SIZE_RECORD[USE]+".txt"
    DATA_ITEM = "itemIndex"+SIZE_RECORD[USE]+".txt"
    DATA_USER = "userIndex"+SIZE_RECORD[USE]+".txt"
    start_time = time.time()
    usr_item_rating = sc.textFile(DATADIR + DATA_All);
    usr_item_rating_count = phase1(usr_item_rating);
    pairs = phase2(usr_item_rating_count)
    result = phase3(pairs)
    resule = result.collect()
    end_time = time.time()
    print "result saved under " + "item_based_"+DATA_All
    outPutTime(start_time, end_time)
    result.saveAsTextFile("item_based_"+DATA_All)
    return result;
    sys.exit(0)



if __name__ == "__main__":
    conf = SparkConf()
    conf.setAppName('Recommendation System - item based')
    conf.set('spark.executor.memory', '6g');
    conf.set('spark.driver.memory', '6g');
    conf.set("spark.rdd.compress","true")
          .set("spark.storage.memoryFraction","1")
          .set("spark.core.connection.ack.wait.timeout","6000")
          .set("spark.akka.frameSize","50")
    conf.setMaster('local[4]')
    sc = SparkContext(conf = conf);
    USE = 2
    main()

