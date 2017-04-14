from csv import reader 
import sys
import pandas as pd
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
    # ndf = ndf.withColumn("duration", timediff
    borough = ["MANHATTAN","BROOKLYN","BRONX","QUEENS","STATEN ISLAND"]
    precinct = [1,5,9,6,7,13,10,14,18,17,20,24,19,26,30,28,32,33,23,25,34,22,40,41,42,44,46,48,52,50,43,49,45,47,90,94,84,88,
79,81,83,75,76,78,72,77,71,68,62,66,60,70,67,61,73,63,69,114,115,108,110,104,112,102,109,107,106,113,111,105,103,100,101,120,122,123,121]
    dict = {"MANHATTAN":[1,5,9,6,7,13,10,14,18,17,20,24,19,26,30,28,32,33,23,25,34,22],"BROOKLYN":[90,94,84,88,79,81,83,75,76,78,72,77,71,68,62,66,60,70,67,61,73,63,69],
"BRONX":[40,41,42,44,46,48,52,50,43,49,45,47],"QUEENS":[114,115,108,110,104,112,102,109,107,106,113,111,105,103,100,101],"STATEN ISLAND":[120,122,123,121]}
    
     
    v0   = lines.map(lambda x: x[0])
    output0 = v0.map(lambda x :('NaN'+' INT ID NULL') if pd.isnull(x) else (str(x)+" INT ID VALID"))
    v135 = lines.map(lambda x: (x[1],x[3],x[5]))
    output1 = v135.map(lambda x :("NaN"+" DATETIME date NULL") if len(x[0])==0 else (str(x[0])+" DATETIME date VALID") if (len(x[1])==0 and int(x[0][-4:])>=1900 and (datetime.strptime(x[2], '%m/%d/%Y')-datetime.strptime(x[0], '%m/%d/%Y')).total_seconds()>=0) else (str(x[0])+" DATETIME date VALID") if (int(x[0][-4:])>=1900 and (datetime.strptime(x[1], '%m/%d/%Y')-datetime.strptime(x[0], '%m/%d/%Y')).total_seconds()>=0 and (datetime.strptime(x[2], '%m/%d/%Y')-datetime.strptime(x[0], '%m/%d/%Y')).total_seconds()>=0) else (str(x[0])+" DATETIME date INVALID"))
    output3 = v135.map(lambda x :("NaN"+" DATETIME date NULL") if len(x[1])==0 else (str(x[0])+" DATETIME date VALID") if (len(x[0])==0 and int(x[1][-4:])< 2020)else (str(x[1])+" DATETIME date VALID") if (int(x[1][-4:])< 2020 and (datetime.strptime(x[1], '%m/%d/%Y')-datetime.strptime(x[0], '%m/%d/%Y')).total_seconds()>=0) else (str(x[1])+" DATETIME date INVALID"))
    output5 = v135.map(lambda x: (str(x[0])+" DATETIME date VALID") if len(x[0])==0 else (str(x[0])+" DATETIME date VALID") if (datetime.strptime(x[2], '%m/%d/%Y')-datetime.strptime(x[0], '%m/%d/%Y')).total_seconds()>=0 else (str(x[0])+" DATETIME date INVALID"))
    v2 = lines.map(lambda x: x[2])
    output2 = v2.map(lambda x : ("NaN"+" DATETIME time NULL") if len(x)==0 else (str(x)+" DATETIME time INVALID") if x=="24:00:00" else (str(x)+" DATETIME time VALID"))
    v4 = lines.map(lambda x: x[4])
    output4 = v4.map(lambda x : ("NaN"+" DATETIME time NULL") if len(x)==0 else (str(x)+" DATETIME time INVALID") if x=="24:00:00" else (str(x)+" DATETIME time VALID"))
    #v1234 = lines.map(lambda x: (x[1],x[2],x[3],x[4]))
    #v1234 = v1234.map(lambda x: (x[0],x[1],x[2],x[3]) if x[1]!= "24:00:00" and x[3]!="24:00:00" else (x[0],"00:00:00",x[2],x[3]) if x[3]!="24:00:00" else (x[0],x[1],x[2],"00:00:00") if x[1]!="24:00:00" else (x[0],"00:00:00",x[2],"00:00:00"))
    #output12 = v1234.map(lambda x: ("NaN"+" DATETIME datetime NULL") if (len(x[0])==0 or len(x[1])==0) else (str(x[0])+" "+str(x[1])+" DATETIME datetime INVALID") if x[0]=="24:00:00" or x[3]=="24:00:00" else (str(x[0])+" "+str(x[1])+" DATETIME datetime VALID") if (len(x[2])==0 or len(x[3])==0) else  (str(x[0])+" "+str(x[1])+" DATETIME datetime VALID") if (datetime.strptime(x[2]+" "+x[3], '%m/%d/%Y %H:%M:%S')-datetime.strptime(x[0]+" "+x[1], '%m/%d/%Y %H:%M:%S')).total_seconds()>=0 else (str(x[0])+" "+str(x[1])+" DATETIME datetime INVALID"))
    #output34 = v1234.map(lambda x: ("NaN"+" DATETIME datetime NULL") if (len(x[2])==0 or len(x[3])==0) else (str(x[0])+" "+str(x[1])+" DATETIME datetime INVALID") if x[0]=="24:00:00" or x[3]=="24:00:00" else (str(x[2])+" "+str(x[3])+" DATETIME datetime VALID") if (len(x[0])==0 or len(x[1])==0) else  (str(x[2])+" "+str(x[3])+" DATETIME datetime VALID") if (datetime.strptime(x[2]+" "+x[3], '%m/%d/%Y %H:%M:%S')-datetime.strptime(x[0]+" "+x[1], '%m/%d/%Y %H:%M:%S')).total_seconds()>=0 else (str(x[2])+" "+str(x[3])+" DATETIME datetime INVALID"))
    v6 = lines.map(lambda x: x[6])
    output6 = v6.map(lambda x : str(x)+" INT category VALID") 
    v7 = lines.map(lambda x: x[7])
    output7 = v7.map(lambda x : (str(x)+" TEXT description VALID") if len(x)>0 else ("NaN"+" TEXT description NULL"))
    v8 = lines.map(lambda x: x[8])
    output8 = v8.map(lambda x : str(x)+" INT category VALID") 
    v9 = lines.map(lambda x: x[9])
    output9 = v9.map(lambda x : (str(x)+" TEXT description VALID") if len(x)>0 else ("NaN"+" TEXT description NULL"))
    v10 =  lines.map(lambda x: x[10])
    output10 = v10.map(lambda x : (str(x)+" TEXT indicator VALID") if len(x)>0 else ("NaN"+" TEXT indicator NULL"))
    v11 =  lines.map(lambda x: x[11])
    output11 = v11.map(lambda x : (str(x)+" TEXT level VALID") if len(x)>0 else ("NaN"+" TEXT level NULL"))
    v12 =  lines.map(lambda x: x[12])
    output12 = v12.map(lambda x : (str(x)+" TEXT discription VALID") if len(x)>0 else ("NaN"+" TEXT discription NULL"))
    v1314 = lines.map(lambda x: (x[13],x[14]))
    output13 = v1314.map(lambda x : ("NaN"+" TEXT location NULL") if len(x[0])==0 else (str(x[0])+" TEXT location VALID") if (x[0] in borough and len(x[1])==0) else (str(x[0])+" TEXT location VALID") if (x[0] in borough and float(x[1]) in dict[x[0]]) else (str(x[0])+ "TEXT category INVALID"))     
    output14 = v1314.map(lambda x : ("NaN"+" FLOAT location NULL") if len(x[1])==0 else (str(x[1])+" FLOAT location VALID") if (float(x[1]) in precinct and len(x[0])==0) else (str(x[1])+" FLOAT location VALID") if (float(x[1]) in precinct and float(x[1]) in dict[x[0]]) else (str(x[1])+ "FlOAT location INVALID"))    
    v15 = lines.map(lambda x: x[15])
    output15 = v15.map(lambda x : (str(x)+" TEXT location discription VALID") if x.lower() in ['inside', 'opposite of', 'front of', 'rear of'] else ("NaN"+" TEXT location discription NULL"))
    v16 = lines.map(lambda x: x[16])
    output16 = v16.map(lambda x : (str(x)+" TEXT premises discription VALID") if len(x)>0 else ("NaN"+" TEXT premises discription NULL"))
    v17 = lines.map(lambda x: x[17])
    output17 = v17.map(lambda x : (str(x)+" TEXT description VALID") if len(x)>0 else ("NaN"+" TEXT description NULL"))
    v18 = lines.map(lambda x: x[18])
    output18 = v18.map(lambda x : (str(x)+" TEXT description VALID") if len(x)>0 else ("NaN"+" TEXT description NULL"))
    v_location = lines.map(lambda x: (x[19],x[20],x[21],x[22]))
    output19 = v_location.map(lambda x : ("NaN"+" FLOAT X_Cordinate NULL") if len(x[0])==0 else (str(x[0])+" FLOAT X_Cordinate INVALID") if (len(x[0])>0 and (len(x[1])==0 or len(x[2])==0 or len(x[3])==0)) else (str(x[0])+" FLOAT X_Cordinate VALID") )
    output20 = v_location.map(lambda x : ("NaN"+" FLOAT Y_Cordinate NULL") if len(x[1])==0 else (str(x[1])+" FLOAT Y_Cordinate INVALID") if (len(x[1])>0 and (len(x[0])==0 or len(x[2])==0 or len(x[3])==0)) else (str(x[1])+" FLOAT Y_Cordinate VALID"))
    output21 = v_location.map(lambda x : ("NaN"+" FLOAT Latitude NULL") if len(x[2])==0 else (str(x[2])+" FLOAT Latitude INVALID") if (len(x[2])>0 and (len(x[0])==0 or len(x[1])==0 or len(x[3])==0)) else (str(x[2])+" FLOAT Latitude VALID"))
    output22 = v_location.map(lambda x : ("NaN"+" FLOAT Longitude NULL") if len(x[3])==0 else (str(x[3])+" FLOAT Longitude INVALID") if (len(x[3])>0 and (len(x[0])==0 or len(x[1])==0 or len(x[2])==0)) else (str(x[3])+" FLOAT Longitude VALID"))
    v23 = lines.map(lambda x: x[23])
    output23 = v23.map(lambda x : (str(x)+" FLOAT Coordinate VALID") if len(x)>0 else ("NaN"+" FLOAT Coordinate NULL"))
    output = output0.union(output1).union(output2).union(output3).union(output4).union(output5).union(output6).union(output7).union(output8).union(output9).union(output10).union(output11).union(output12).union(output13).union(output14).union(output15).union(output16).union(output17).union(output18).union(output19).union(output20).union(output21).union(output22).union(output23)
    output.saveAsTextFile("test.out")