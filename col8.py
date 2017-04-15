from csv import reader 
import sys
from datetime import datetime
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1) 
    lines = lines.mapPartitions(lambda x: reader(x))
    header = lines.first()
    lines = lines.filter(lambda line: line != header)
    v8 = lines.map(lambda x: x[8])
    output8 = v8.map(lambda x : str(x)+" INT category VALID") 
    output8.saveAsTextFile("test8.out")