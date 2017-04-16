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
	v11 =  lines.map(lambda x: x[11])
	output11 = v11.map(lambda x : (str(x)+" TEXT level VALID") if len(x)>0 else ("NaN"+" TEXT level NULL"))
	output11.saveAsTextFile("test11.out")

