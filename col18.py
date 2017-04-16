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
    
    v18 = lines.map(lambda x: x[18])
    output18 = v18.map(lambda x : (str(x)+" TEXT description VALID") if len(x)>0 else ("NaN"+" TEXT description NULL"))
    
    output18.saveAsTextFile("test18.out")