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
	f1 = lines.filter(lambda x : int(x[1][-4:])>=1900 or len(x[1])==0)
	f2 = f1.filter(lambda x : int(x[2][-4:])<2020 or len(x[2])==0)