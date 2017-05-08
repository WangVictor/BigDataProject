import folium
import pandas as pd
from IPython.core.display import display, HTML
from folium.plugins import MarkerCluster
NYC_COORDINATES = (40.7128,-74.0059)
collisiondata = pd.read_csv('NYPD_Motor_Vehicle_Collisions.csv').dropna(subset=['LATITUDE','LONGITUDE'])
# for speed purposes
MAX_RECORDS = 1000
# create empty map zoomed in on San Francisco
NYC = folium.Map(location=NYC_COORDINATES, zoom_start=12)
#marker_cluster = folium.MarkerCluster("Vehicle Collisions Cluster").add_to(NYC)
# add a marker for every record in the filtered data, use a clustered view
lons=[]
lats=[]
for each in collisiondata[0:MAX_RECORDS].iterrows():
    lon = each[1]['LONGITUDE']
    lons.append(lon) 
    lat = each[1]['LATITUDE'] 
    lats.append(lat) 
locations = list(zip(lats,lons))
popups = ['{}'.format(loc) for loc in locations]
NYC.add_child(MarkerCluster(locations=locations, popups=popups))
display(NYC)
NYC.save('collision_interactive.html')