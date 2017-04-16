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
	v12 =  lines.map(lambda x: x[12])
	output12 = v12.map(lambda x : (str(x)+" TEXT discription VALID") if len(x)>0 else ("NaN"+" TEXT discription NULL"))
	output12.saveAsTextFile("test12.out")

	