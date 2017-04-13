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
	# date = lines.map(lambda x: Row(CMPLNT_FR_DT=x[1], CMPLNT_FR_TM=x[2],CMPLNT_TO_DT=x[3],CMPLNT_TO_TM=x[4]))
	# date=sqlContext.createDataFrame(date)
	# ndf = date.withColumn('CMPLNT_TO_DT', date['CMPLNT_TO_DT'].cast(DateType()))
	# ndf = ndf.withColumn('CMPLNT_FR_DT', ndf['CMPLNT_FR_DT'].cast(DateType()))
	# timediff=(ndf['CMPLNT_TO_DT']-ndf['CMPLNT_FR_DT'])
	# ndf = ndf.withColumn("duration", timediff)
	v135 = lines.map(lambda x: (x[1],x[3],x[5]))
	output1 = v135.map(lambda x :("NaN"+" DATETIME date NULL") if len(x[0])==0 else (str(x[0])+" DATETIME date VALID") if (len(x[1])==0 and int(x[0][-4:])>=1900 and (datetime.strptime(x[2], '%m/%d/%Y')-datetime.strptime(x[0], '%m/%d/%Y')).total_seconds()>=0) else (str(x[0])+" DATETIME date VALID") if (int(x[0][-4:])>=1900 and (datetime.strptime(x[1], '%m/%d/%Y')-datetime.strptime(x[0], '%m/%d/%Y')).total_seconds()>=0 and (datetime.strptime(x[2], '%m/%d/%Y')-datetime.strptime(x[0], '%m/%d/%Y')).total_seconds()>=0) else (str(x[0])+" DATETIME date INVALID"))
	output3 = v135.map(lambda x :("NaN"+" DATETIME date NULL") if len(x[1])==0 else (str(x[0])+" DATETIME date VALID") if (len(x[0])==0 and int(x[1][-4:])< 2020)else (str(x[1])+" DATETIME date VALID") if (int(x[1][-4:])< 2020 and (datetime.strptime(x[1], '%m/%d/%Y')-datetime.strptime(x[0], '%m/%d/%Y')).total_seconds()>=0) else (str(x[1])+" DATETIME date INVALID"))
	output5 = v135.map(lambda x: (str(x[0])+" DATETIME date VALID") if len(x[0])==0 else (str(x[0])+" DATETIME date VALID") if (datetime.strptime(x[2], '%m/%d/%Y')-datetime.strptime(x[0], '%m/%d/%Y')).total_seconds()>=0 else (str(x[0])+" DATETIME date INVALID"))
	v2 = lines.map(lambda x: x[2])
	output2 = v2.map(lambda x : ("NaN"+" DATETIME time NULL") if len(x)==0 else (str(x)+" DATETIME time INVALID") if x=="24:00:00" else (str(x)+" DATETIME time VALID"))
	v4 = lines.map(lambda x: x[4])
	output4 = v4.map(lambda x : ("NaN"+" DATETIME time NULL") if len(x)==0 else (str(x)+" DATETIME time INVALID") if x=="24:00:00" else (str(x)+" DATETIME time VALID"))
	# v1234 = lines.map(lambda x: (x[1],x[2],x[3],x[4]))
	# v1234 = v1234.map(lambda x: (x[0],x[1],x[2],x[3]) if x[1]!= "24:00:00" and x[3]!="24:00:00" else (x[0],"00:00:00",x[2],x[3]) if x[3]!="24:00:00" else (x[0],x[1],x[2],"00:00:00") if x[1]!="24:00:00" else (x[0],"00:00:00",x[2],"00:00:00"))
	# output12 = v1234.map(lambda x: ("NaN"+" DATETIME datetime NULL") if (len(x[0])==0 or len(x[1])==0) else (str(x[0])+" "+str(x[1])+" DATETIME datetime VALID") if (len(x[2])==0 or len(x[3])==0) else  (str(x[0])+" "+str(x[1])+" DATETIME datetime VALID") if (datetime.strptime(x[2]+" "+x[3], '%m/%d/%Y %H:%M:%S')-datetime.strptime(x[0]+" "+x[1], '%m/%d/%Y %H:%M:%S')).total_seconds()>=0 else (str(x[0])+" "+str(x[1])+" DATETIME datetime INVALID"))
	# output34 = v1234.map(lambda x: ("NaN"+" DATETIME datetime NULL") if (len(x[2])==0 or len(x[3])==0) else (str(x[2])+" "+str(x[3])+" DATETIME datetime VALID") if (len(x[0])==0 or len(x[1])==0) else  (str(x[2])+" "+str(x[3])+" DATETIME datetime VALID") if (datetime.strptime(x[2]+" "+x[3], '%m/%d/%Y %H:%M:%S')-datetime.strptime(x[0]+" "+x[1], '%m/%d/%Y %H:%M:%S')).total_seconds()>=0 else (str(x[2])+" "+str(x[3])+" DATETIME datetime INVALID"))
	v6 = lines.map(lambda x: x[6])
	output6 = v6.map(lambda x : str(x)+" INT category VALID") 
	v7 = lines.map(lambda x: x[7])
	output7 = v7.map(lambda x : (str(x)+" TEXT description VALID") if len(x)>0 else ("NaN"+" TEXT description NULL"))
	v8 = lines.map(lambda x: x[8])
	output8 = v8.map(lambda x : str(x)+" INT category VALID") 
	v9 = lines.map(lambda x: x[9])
	output9 = v9.map(lambda x : (str(x)+" TEXT description VALID") if len(x)>0 else ("NaN"+" TEXT description NULL"))
	output = output1.union(output2).union(output3).union(output4).union(output5).union(output6).union(output7).union(output8).union(output9)
	output.saveAsTextFile("test.out")