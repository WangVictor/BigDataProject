# This scrip was designed for exploring the relationship between Crimes and weather data
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt  
import datetime 
tableau20 = [(31, 119, 180), (174, 199, 232), (255, 127, 14), (255, 187, 120),    
             (44, 160, 44), (152, 223, 138), (214, 39, 40), (255, 152, 150),    
             (148, 103, 189), (197, 176, 213), (140, 86, 75), (196, 156, 148),    
             (227, 119, 194), (247, 182, 210), (127, 127, 127), (199, 199, 199),    
             (188, 189, 34), (219, 219, 141), (23, 190, 207), (158, 218, 229)]    
  
# Scale the RGB values to the [0, 1] range, which is the format matplotlib accepts.    
for i in range(len(tableau20)):    
    r, g, b = tableau20[i]    
    tableau20[i] = (r / 255., g / 255., b / 255.)  
#Load Clean Crime Data
print('Loading Data....\n')
data = pd.read_csv('./NYPD_Complaint_Data_Historic.csv')
data['CMPLNT_FR_DT'] = data['CMPLNT_FR_DT'][~pd.isnull(data['CMPLNT_FR_DT'].values)].str[:].map(lambda dates: pd.datetime.strptime(dates, '%m/%d/%Y'))
print('Data loaded\n')
#Load and clean weather data
weather_data_head = pd.read_csv('./weather.header')
weather_data = pd.read_csv('./weather.csv',names = weather_data_head.columns.values)
weather_data['HrMn[Identification]'] = weather_data['HrMn[Identification]'].str[1:-1].map(lambda x : x[0]+x[1]+':'+x[2]+x[3]+':'+'00')
weather_data['Date[Identification]'] = weather_data['Date[Identification]'].str[1:-1].map(lambda x : x[4:6]+'/'+x[6:]+'/'+x[0:4])

data= data[data['CMPLNT_FR_DT'] > datetime.datetime(2010, 1, 1, 0, 0)]
weather_data['Date[Identification]'] = weather_data['Date[Identification]'].str[:].map(lambda dates: pd.datetime.strptime(dates, '%m/%d/%Y'))
weather_data = weather_data[weather_data['Date[Identification]']>datetime.datetime(2010, 1, 1, 0, 0)]

#Calculate daily temp and daily wind speed 
daily_temp = weather_data.groupby(by = 'Date[Identification]')['Temp[Temp]'].mean()
daily_wind_speed = weather_data.groupby(by = 'Date[Identification]')['Spd[Wind]'].median()
daily_data = data.groupby(by = 'CMPLNT_FR_DT')['CMPLNT_NUM'].count()
plt.figure(figsize=(10,3))
plt.plot(daily_temp.index.values,daily_wind_speed.values)
plt.title('NYC daily median wind speed')
plt.show()

#Find extrem wind speed
m = daily_wind_speed[daily_wind_speed.values>10].index.values
plt.figure(figsize=(10,3))
#plt.plot([1353.244292237443]*len(daily_data[m]))
plt.plot(daily_data[m],label = 'Crimes in days with strong winds', color = tableau20[2])
plt.plot(daily_data[m].index.values,[1353]*len(daily_data[m]),label = 'Average Crimes',color = tableau20[1])
plt.legend(loc = 'best')
plt.show()
#Plot temperature 
plt.figure(figsize=(10,3))
plt.plot(daily_temp.index.values,daily_temp.values,color = tableau20[19])
plt.title('NYC Temperature from 2010 - 2014')
plt.show()
#plot Crimes
plt.figure(figsize=(10,3))
#plt.plot(daily_temp.index.values,daily_temp.values)
plt.plot(daily_data.index.values,daily_data.values, color = tableau20[5])
plt.title('NYC Crimes Frequency from 2010 - 2014')
plt.show()

corr = daily_temp.corr(daily_data.reindex(daily_temp.index, method='pad'))
print('The correlation between Temperature and Crimes is %.2f'%corr )