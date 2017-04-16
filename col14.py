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
    borough = ["MANHATTAN","BROOKLYN","BRONX","QUEENS","STATEN ISLAND"]
    precinct = [1,5,9,6,7,13,10,14,18,17,20,24,19,26,30,28,32,33,23,25,34,22,40,41,42,44,46,48,52,50,43,49,45,47,90,94,84,88,
79,81,83,75,76,78,72,77,71,68,62,66,60,70,67,61,73,63,69,114,115,108,110,104,112,102,109,107,106,113,111,105,103,100,101,120,122,123,121]
    dict = {"MANHATTAN":[1,5,9,6,7,13,10,14,18,17,20,24,19,26,30,28,32,33,23,25,34,22],"BROOKLYN":[90,94,84,88,79,81,83,75,76,78,72,77,71,68,62,66,60,70,67,61,73,63,69],
"BRONX":[40,41,42,44,46,48,52,50,43,49,45,47],"QUEENS":[114,115,108,110,104,112,102,109,107,106,113,111,105,103,100,101],"STATEN ISLAND":[120,122,123,121]}
    
    v1314 = lines.map(lambda x: (x[13],x[14]))
    output14 = v1314.map(lambda x : ("NaN"+" FLOAT location NULL") if len(x[1])==0 else (str(x[1])+" FLOAT location VALID") if (float(x[1]) in precinct and len(x[0])==0) else (str(x[1])+" FLOAT location VALID") if (float(x[1]) in precinct and float(x[1]) in dict[x[0]]) else (str(x[1])+ "FlOAT location INVALID"))
    
    output14.saveAsTextFile("test14.out")