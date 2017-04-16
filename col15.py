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
	v15 = lines.map(lambda x: x[15])
	output15 = v15.map(lambda x : (str(x)+" TEXT location discription VALID") if x.lower() in ['inside', 'opposite of', 'front of', 'rear of'] else ("NaN"+" TEXT location discription NULL"))
	output15.saveAsTextFile("test15.out")

	
	