#splits all punctuations meaning "they'll" will become 
#"they" and "ll"

from pyspark import SparkContext
import sys
import re
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print >> sys.stderr, "Usage: average.py <input> <stopwords> <output>"
        exit(-1)
    sc = SparkContext()
    rdd = sc.textFile(sys.argv[1])
    stop_w = sc.textFile(sys.argv[2])
    stop_w = stop_w.flatMap(lambda stop: stop.split('\\W')) \
        .map(lambda word: (word, 1))
   
    rdd = rdd.flatMap(lambda line: re.split('\\s+', line)) \
        .map(lambda lower: lower.lower())\
        .filter(lambda filt: len(filt) > 0) \
        .map(lambda x: (x,1)) \
        .reduceByKey(lambda x, y: x+y) \
        .subtractByKey(stop_w)
    final_RDD = rdd.map(lambda x: (x[1], x[0])).sortByKey(False).take(10)
    for i in final_RDD:
        print(i)
    sc.stop()
