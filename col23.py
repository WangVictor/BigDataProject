from csv import reader 
import sys
from datetime import datetime
from pyspark import SparkContext

# from pyspark.sql import Row

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1) 
    lines = lines.mapPartitions(lambda x: reader(x))
    header = lines.first()
    lines = lines.filter(lambda line: line != header)
    
    v23 = lines.map(lambda x: x[23])
    output23 = v23.map(lambda x : (str(x)+" FLOAT Coordinate Pair VALID") if len(x)>0 else ("NaN"+" FLOAT Coordinate Pair NULL"))

    output23.saveAsTextFile("test23.out")