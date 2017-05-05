from csv import reader 
import sys
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import StringType
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

#if __name__ == "__main__":
sc = SparkContext()
sqlContext = HiveContext(sc)
lines = sc.textFile("rows.csv", 1) 
lines = lines.mapPartitions(lambda x: reader(x))
header = lines.first()
lines = lines.filter(lambda line: line != header)
f1 = lines.filter(lambda x : int(x[1][-4:])>=1900  if len(x[1])!=0 else True if len(x[1])==0 else False)
f2 = f1.filter(lambda x : int(x[3][-4:])<2020 if len(x[3])!=0 else True if len(x[3])==0 else False)
f3 = f2.filter(lambda x: (datetime.strptime(x[3], '%m/%d/%Y')-datetime.strptime(x[1], '%m/%d/%Y')).total_seconds()>=0 if len(x[3])!=0 and len(x[1])!=0 else True if len(x[1])==0 or len(x[3])==0 else False)
f4 = f3.filter(lambda x: (datetime.strptime(x[5], '%m/%d/%Y')-datetime.strptime(x[1], '%m/%d/%Y')).total_seconds()>=0 if len(x[1])!=0 else True if len(x[1])==0 else False)
f5 = f4.filter(lambda x:x[2]!= "24:00:00")
f6 = f5.filter(lambda x:x[4]!= "24:00:00")
#m1 = f4.map(lambda x: x if x[2]!= "24:00:00" and x[4]!="24:00:00" else x[0:2]+["00:00:00"]+x[3:] if x[4]!="24:00:00" else x[0:4]+["00:00:00"]+x[5:] if x[2]!="24:00:00" else x[0:2]+["00:00:00"]+x[3:4]+["00:00:00"]+x[5:])
f7 = f6.filter(lambda x: (datetime.strptime(x[3]+" "+x[4], '%m/%d/%Y %H:%M:%S')-datetime.strptime(x[1]+" "+x[2], '%m/%d/%Y %H:%M:%S')).total_seconds()>=0 if len(x[1])!=0 and len(x[3])!=0 and len(x[2])!=0 and len(x[4])!= 0 else True if len(x[1])==0 or len(x[3])==0 or len(x[2])!=0 or len(x[4])!= 0 else False)
m1 = f7.map(lambda x: x[0:7]+["OTHER STATE LAWS"]+x[8:] if x[6]=='364' else x)
m2 = m1.map(lambda x: x[0:7]+["KIDNAPPING"]+x[8:] if x[6]=='124' else x)
m3 = m2.map(lambda x: x[0:7]+["CHILD ABANDONMENT"]+x[8:] if x[6]=='120' else x)
m4 = m3.map(lambda x: x[0:7]+["NYS LAWS-UNCLASSIFIED VIOLATION"]+x[8:] if x[6]=='677' else x)
m5 = m4.map(lambda x: x[0:7]+["ENDAN WELFARE INCOMP"]+x[8:] if x[6]=='345' else x)
lines = m5.map(lambda x: x[0:7]+["OTHER OFFENSES RELATED TO THEF"]+x[8:] if x[6]=='343' else x)
lines = lines.map(lambda x:x+[x[1]+" "+x[2]] if len(x[1])!=0 and len(x[2])!= 0 else x+[""])
crime_df_schema = StructType(
  [StructField('CMPLNT_NUM', StringType()),
   StructField('FR_HR', StringType()),
   StructField('CRM_ATPT_CPTD_CD', StringType()),
   StructField('LAW_CAT_CD', StringType()),
   StructField('ADDR_PCT_CD', StringType()),
   ]
)