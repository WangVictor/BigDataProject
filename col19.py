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
    
    v_location = lines.map(lambda x: (x[19],x[20],x[21],x[22]))
    output19 = v_location.map(lambda x : ("NaN"+" FLOAT X_Cordinate NULL") if len(x[0])==0 else (str(x[0])+" FLOAT X_Cordinate INVALID") if (len(x[0])>0 and (len(x[1])==0 or len(x[2])==0 or len(x[3])==0)) else (str(x[0])+" FLOAT X_Cordinate VALID") )
    
    output19.saveAsTextFile("test19.out")