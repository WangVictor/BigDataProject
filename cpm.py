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
# lines = sc.textFile(sys.argv[1], 1) 
# lines = lines.mapPartitions(lambda x: reader(x))
# header = lines.first()
# lines = lines.filter(lambda line: line != header)
lines = lines.map(lambda x:x+[x[1]+" "+x[2]] if len(x[1])!=0 and len(x[2])!= 0 else x+["01/01/1900 12:00:00"])
#lines = lines.map(lambda x:x+[x[1]+" "+x[2][0:3]+"00:00"])

crime_df_schema = StructType(
  [StructField('CMPLNT_NUM', StringType()),
   StructField('FR_HR', StringType()),
   StructField('CRM_ATPT_CPTD_CD', StringType()),
   StructField('LAW_CAT_CD', StringType()),
   StructField('ADDR_PCT_CD', StringType()),
   ]
)
lines = lines.map(lambda x: Row(CMPLNT_NUM=x[0], FR_HR=x[-1],CRM_ATPT_CPTD_CD=x[10],LAW_CAT_CD=x[11],ADDR_PCT_CD=x[14]))
crime_df = sqlContext.createDataFrame(lines,schema=crime_df_schema)
def blank_as_null(x):
  return when(col(x) != "", col(x)).otherwise(None)
string_fields = []
for i, f in enumerate(crime_df.schema.fields):
  if isinstance(f.dataType, StringType):
    string_fields.append(f.name)
exprs = [
  blank_as_null(x).alias(x) if x in string_fields else x for x in crime_df.columns]
crime_df = crime_df.select(*exprs)
crime_df = crime_df.na.drop()
crime_df = crime_df.where(F.length(col("FR_HR")) >= 14)
func =  udf (lambda x: datetime.strptime(x, '%m/%d/%Y %H:%M:%S'), TimestampType())

crime_df = crime_df.withColumn('FR_HR', func(col('FR_HR')))
crime_df.show(10)
# crime_df.withColumn("FR_HR", F.unix_timestamp("FR_HR")).agg(
#   F.from_unixtime(min("FR_HR")).alias("min_ts"), 
#   F.from_unixtime(max("FR_HR")).alias("max_ts")
# ).show()

## create training and test data
train_date_from = datetime(2010,1,1,0,0)
test_date_from = datetime(2014,1,1,0,0)
test_date_to =datetime(2016,1,1,0,0,0)
train_data = crime_df.filter(crime_df.FR_HR >= train_date_from).filter(crime_df.FR_HR < test_date_from)
test_data = crime_df.filter(crime_df.FR_HR >= test_date_from).filter(crime_df.FR_HR < test_date_to)
train_data = train_data.withColumn('hour',F.hour('FR_HR'))
test_data = test_data.withColumn('hour',F.hour('FR_HR'))
train_group = train_data.groupBy(['ADDR_PCT_CD','hour','LAW_CAT_CD']).count()
test_group = test_data.groupBy(['ADDR_PCT_CD','hour','LAW_CAT_CD']).count()
train_pd = train_group.toPandas()
test_pd = test_group.toPandas()
### plot precinct 5
plotdf = train_pd[train_pd.ADDR_PCT_CD=='5'].sort('hour')
x = range(24)
plt.clf()
plt.plot(x, plotdf[plotdf.LAW_CAT_CD=='VIOLATION']['count'])
plt.plot(x, plotdf[plotdf.LAW_CAT_CD=='FELONY']['count'])
plt.plot(x, plotdf[plotdf.LAW_CAT_CD=='MISDEMEANOR']['count'])
plt.legend(['VIOLATION', 'FELONY', 'MISDEMEANOR'], loc='upper left')
plt.title('Precinct 5 Day Hour Crime Counts')
plt.xlabel('hour')
plt.ylabel('counts')
plt.savefig('cpm.png')

join_pd = train_pd.merge(test_pd,how='inner',on=['ADDR_PCT_CD','hour','LAW_CAT_CD'])
join_pd = join_pd.sort(['ADDR_PCT_CD','hour','LAW_CAT_CD'])
train_grouped = join_pd[['ADDR_PCT_CD','hour','count_x']].groupby(['ADDR_PCT_CD','hour'])
test_grouped = join_pd[['ADDR_PCT_CD','hour','count_y']].groupby(['ADDR_PCT_CD','hour'])
train_df = train_grouped['count_x'].apply(list)
test_df = test_grouped['count_y'].apply(list)
## Only keep list length 3
drop_train=[]
for i in range(len(train_df)):
  if len(train_df[i])!=3:
    drop_train.append(i)
drop_test=[]
for i in range(len(test_df)):
  if len(test_df[i])!=3:
    drop_test.append(i)
train_df = train_df.drop(train_df.index[drop_train])
test_df = test_df.drop(test_df.index[drop_test])

train_fel = []
for i in range(len(train_df)):
  train_fel.append(train_df[i][0]/sum(train_df[i]))
train_mis = []
for i in range(len(train_df)):
  train_mis.append(train_df[i][1]/sum(train_df[i]))
train_vio = []
for i in range(len(train_df)):
  train_vio.append(train_df[i][2]/sum(train_df[i]))
test_fel = []
for i in range(len(test_df)):
  test_fel.append(test_df[i][0]/sum(test_df[i]))
test_mis = []
for i in range(len(test_df)):
  test_mis.append(test_df[i][1]/sum(test_df[i]))
test_vio = []
for i in range(len(test_df)):
  test_vio.append(test_df[i][2]/sum(test_df[i]))
KL = []
for i in range(len(train_fel)):
  KL.append(train_fel[i]*np.log2(train_fel[i]/test_fel[i])+train_mis[i]*np.log2(train_mis[i]/test_mis[i])+train_vio[i]*np.log2(train_vio[i]/test_vio[i]))
np.mean(KL)
### 0.013288107158600399



plt.clf()
pdDF = perc_group.toPandas()
pdDF.plot(x='ADDR_PCT_CD', y='count', kind='bar', rot=45)

crime_df.select(F.year('FR_HR').alias('year')).collect()

crime_df.printSchema()