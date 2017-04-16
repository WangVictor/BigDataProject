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
	v16 = lines.map(lambda x: x[16])
	output16 = v16.map(lambda x : (str(x)+" TEXT premises discription VALID") if len(x)>0 else ("NaN"+" TEXT premises discription NULL"))
	output16.saveAsTextFile("test16.out")

	
	