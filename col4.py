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
    v4 = lines.map(lambda x: x[4])
    output4 = v4.map(lambda x : ("NaN"+" DATETIME time NULL") if len(x)==0 else (str(x)+" DATETIME time INVALID") if x=="24:00:00" else (str(x)+" DATETIME time VALID"))
    output4.saveAsTextFile("test4.out")