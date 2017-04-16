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
    v0   = lines.map(lambda x: x[0])
    output0 = v0.map(lambda x :('NaN'+' INT ID NULL') if pd.isnull(x) else (str(x)+" INT ID VALID"))
    output0.saveAsTextFile("test0.out")