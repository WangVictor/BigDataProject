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
## read crime data
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
lines = lines.map(lambda x:x+[x[1]+" "+x[2][0:3]+"00:00"] if len(x[1])!=0 and len(x[2])!= 0 else x+["01/01/1900 12:00:00"])
## create crime dataframe
crime_df_schema = StructType(
  [StructField('CMPLNT_NUM', StringType()),
   StructField('FR_HR', StringType()),
   StructField('LATITUDE', StringType()),
   StructField('LONGITUDE', StringType())
   ]
)
lines = lines.map(lambda x: Row(CMPLNT_NUM=x[0], FR_HR=x[-1],LATITUDE=x[-4],LONGITUDE=x[-3]))
crime_df = sqlContext.createDataFrame(lines,schema=crime_df_schema)
def blank_as_null(x):
  return when(col(x) != "", col(x)).otherwise(None)
string_fields = ['CMPLNT_NUM','FR_HR','LATITUDE','LONGITUDE']
exprs = [
  blank_as_null(x).alias(x) if x in string_fields else x for x in crime_df.columns]
crime_df = crime_df.select(*exprs)
crime_df = crime_df.dropna()
crime_df = crime_df.where(F.length(col("FR_HR")) >= 14)
crime_df = crime_df.where(F.length(col("LATITUDE")) >= 6)
crime_df = crime_df.where(F.length(col("LONGITUDE")) >= 6)
func =  udf (lambda x: datetime.strptime(x, '%m/%d/%Y %H:%M:%S'), TimestampType())

crime_df = crime_df.withColumn('FR_HR', func(col('FR_HR')))
crime_df = crime_df.withColumn("LATITUDE",crime_df["LATITUDE"].cast(DoubleType()).alias("LATITUDE"))
crime_df = crime_df.withColumn("LONGITUDE",crime_df["LONGITUDE"].cast(DoubleType()).alias("LONGITUDE"))
crime_df.show(10)
## Subset 2013-2015
date_from = datetime(2013,1,1,0,0)
date_to = datetime(2016,1,1,0,0)
crime_df = crime_df.filter(crime_df.FR_HR >= date_from).filter(crime_df.FR_HR < date_to)
## Truncate Longitude, Latitude
crime_df = crime_df.withColumn("LATITUDE", F.format_number(crime_df.LATITUDE, 2))
crime_df = crime_df.withColumn("LONGITUDE", F.format_number(crime_df.LONGITUDE, 2))

## Plot Time Series
crime_df = crime_df.withColumn('year',F.year('FR_HR'))
crime_2015 = crime_df.filter(crime_df.year==2015)
crime_2015 = crime_2015.withColumn('date',F.to_date('FR_HR'))
crime_2015_group = crime_2015.groupBy('date').count()
crime_2015_pd = crime_2015_group.toPandas()
crime_2015_pd.date = pd.to_datetime(crime_2015_pd.date, format='%Y-%m-%d')
crime_2015_pd = crime_2015_pd.sort(columns='date')
dates = pd.date_range('2015-01-01', '2015-12-31')
AO = crime_2015_pd.ix[:,1]
AO.index = dates
AO.plot()
plt.title('NYC Crime Frequency in 2015')
plt.savefig('2015crime.png') 
plt.close()


## dealing with traffic collision data
lines = sc.textFile("collision.csv", 1) 
lines = lines.mapPartitions(lambda x: reader(x))
header = lines.first()
lines = lines.filter(lambda line: line != header)
lines = lines.map(lambda x:x+[x[0]+" "+x[1][0:3]+"00:00"] if len(x[0])!=0 and len(x[1])!= 0 and len(x[1])==5 else x+[x[0]+" 0" + x[1][0]+":00:00"] if len(x[1])== 4 else x+["01/01/1900 12:00:00"])
collision_df_schema = StructType(
  [StructField('ACCD_NUM', StringType()),
   StructField('FR_HR', StringType()),
   StructField('LATITUDE', StringType()),
   StructField('LONGITUDE', StringType())
   ]
)
lines = lines.map(lambda x: Row(ACCD_NUM=x[-7], FR_HR=x[-1],LATITUDE=x[4],LONGITUDE=x[5]))
collision_df = sqlContext.createDataFrame(lines,schema=collision_df_schema)
string_fields = ['ACCD_NUM','FR_HR','LATITUDE','LONGITUDE']
exprs = [
  blank_as_null(x).alias(x) if x in string_fields else x for x in collision_df.columns]
collision_df = collision_df.select(*exprs)
collision_df = collision_df.dropna()
func =  udf (lambda x: datetime.strptime(x, '%m/%d/%Y %H:%M:%S'), TimestampType())

collision_df = collision_df.withColumn('FR_HR', func(col('FR_HR')))
collision_df = collision_df.withColumn("LATITUDE",collision_df["LATITUDE"].cast(DoubleType()).alias("LATITUDE"))
collision_df = collision_df.withColumn("LONGITUDE",collision_df["LONGITUDE"].cast(DoubleType()).alias("LONGITUDE"))
collision_df.show(10)
## Subset 2013-2015
date_from = datetime(2013,1,1,0,0)
date_to = datetime(2016,1,1,0,0)
collision_df = collision_df.filter(collision_df.FR_HR >= date_from).filter(collision_df.FR_HR < date_to)
## Truncate Longitude, Latitude
collision_df = collision_df.withColumn("LATITUDE", F.format_number(collision_df.LATITUDE, 2))
collision_df = collision_df.withColumn("LONGITUDE", F.format_number(collision_df.LONGITUDE, 2))

## Plot Time Series
collision_df = collision_df.withColumn('year',F.year('FR_HR'))
collision_2015 = collision_df.filter(collision_df.year==2015)
collision_2015 = collision_2015.withColumn('date',F.to_date('FR_HR'))
collision_2015_group = collision_2015.groupBy('date').count()
collision_2015_pd = collision_2015_group.toPandas()
collision_2015_pd.date = pd.to_datetime(collision_2015_pd.date, format='%Y-%m-%d')
collision_2015_pd = collision_2015_pd.sort(columns='date')
dates = pd.date_range('2015-01-01', '2015-12-31')
BO = collision_2015_pd.ix[:,1]
BO.index = dates
BO.plot()
plt.title('NYC Triffic Collision Frequency in 2015')
plt.savefig('2015collision.png') 
plt.close()

## Calculate correlation
AO.corr(BO)
## scatter plot
plt.scatter(AO,BO)
plt.title('Scatter Plot of 2015 NYC Crime and Traffic Collision By Day')
plt.xlabel('Crime')
plt.ylabel('Traffic Collision')
plt.savefig('2015corr.png') 
plt.close()

## Get loc grid
collision_df = collision_df.dropna()
collision_df = collision_df.withColumn('hour',F.hour('FR_HR'))
crime_df = crime_df.withColumn('hour',F.hour('FR_HR'))
collision_mor = collision_df.filter(collision_df.hour>=8).filter(collision_df.hour<=11)
collision_eve = collision_df.filter(collision_df.hour>=17).filter(collision_df.hour<=20)
crime_mor = crime_df.filter(crime_df.hour>=8).filter(crime_df.hour<=11)
crime_eve = crime_df.filter(crime_df.hour>=17).filter(crime_df.hour<=20)
col_mor_grp = collision_mor.groupBy(['LATITUDE','LONGITUDE']).count()
col_eve_grp = collision_eve.groupBy(['LATITUDE','LONGITUDE']).count()
cri_mor_grp = crime_mor.groupBy(['LATITUDE','LONGITUDE']).count()
cri_eve_grp = crime_eve.groupBy(['LATITUDE','LONGITUDE']).count()

col_mor_pd = col_mor_grp.toPandas()
col_eve_pd = col_eve_grp.toPandas()
cri_mor_pd = cri_mor_grp.toPandas()
cri_eve_pd = cri_eve_grp.toPandas()

merge_data = col_mor_pd.merge(col_eve_pd,left_on=['LATITUDE','LONGITUDE'],right_on=['LATITUDE','LONGITUDE']).merge(
	cri_mor_pd,left_on=['LATITUDE','LONGITUDE'],right_on=['LATITUDE','LONGITUDE']).merge(
	cri_eve_pd,left_on=['LATITUDE','LONGITUDE'],right_on=['LATITUDE','LONGITUDE'])



