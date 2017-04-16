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
	v10 =  lines.map(lambda x: x[10])
	output10 = v10.map(lambda x : (str(x)+" TEXT indicator VALID") if len(x)>0 else ("NaN"+" TEXT indicator NULL"))
	output10.saveAsTextFile("test10.out")
