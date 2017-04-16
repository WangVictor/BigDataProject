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
    v7 = lines.map(lambda x: x[7])
    output7 = v7.map(lambda x : (str(x)+" TEXT description VALID") if len(x)>0 else ("NaN"+" TEXT description NULL"))
    output7.saveAsTextFile("test7.out")