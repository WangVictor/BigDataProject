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
    v135 = lines.map(lambda x: (x[1],x[3],x[5]))
    output1 = v135.map(lambda x :("NaN"+" DATETIME date NULL") if len(x[0])==0 else (str(x[0])+" DATETIME date VALID") if (len(x[1])==0 and int(x[0][-4:])>=1900 and (datetime.strptime(x[2], '%m/%d/%Y')-datetime.strptime(x[0], '%m/%d/%Y')).total_seconds()>=0) else (str(x[0])+" DATETIME date VALID") if (int(x[0][-4:])>=1900 and (datetime.strptime(x[1], '%m/%d/%Y')-datetime.strptime(x[0], '%m/%d/%Y')).total_seconds()>=0 and (datetime.strptime(x[2], '%m/%d/%Y')-datetime.strptime(x[0], '%m/%d/%Y')).total_seconds()>=0) else (str(x[0])+" DATETIME date INVALID"))
    output1.saveAsTextFile("test1.out")