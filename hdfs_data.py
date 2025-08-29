
import pandas as pd
from utils import debugInfo,debugDebug,find_common_elements
from config import HTTP_HADOOP_ENDPOINT,WEBHDFS_HADOOP_ENDPOINT,HADOOP_USER

def get_geometadata(p_list_entities):
    from hdfs import InsecureClient
    client = InsecureClient(HTTP_HADOOP_ENDPOINT, user=HADOOP_USER)
    df_total=None
    n=0
    tnames = client.list('/data/',status=False)
    for t in tnames:
        debugDebug(f'Tenant: {t}')
        enames = client.list('/data/'+t,status=False)
        entitiesToLoad=find_common_elements(enames,p_list_entities)
        for entityName in entitiesToLoad:
           tmp_file="/tmp/entity_geometadata_"+t+'_'+entityName+".csv"
           debugInfo(f'Loading geometadata {t} {entityName}...')
           path=WEBHDFS_HADOOP_ENDPOINT+'/data/'+t+'/'+entityName+'/geometadata.parquet'
           debugInfo(f'Creating a Pandas for '+path+'...')
           df=pd.read_parquet(path)
           debugInfo(f'Next!')
           if (n==0):
              df_total=df
           else:
              df_total=pd.concat([df_total,df])
           n=n+1
    return df_total


def get_filtered_services(df,p_list_of_locals,p_list_of_coords):
    print(df)
    services={}
    p=0
    for coord_point in p_list_of_coords:
      debugInfo(f"Start:Check coords {coord_point}  {p_list_of_locals[p]}");
      coords = coord_point.split(",")
      latitude=float(coords[0])
      longitude=float(coords[1])
      debugInfo(f"Latitude={latitude}  Longitude={longitude}")
      for index, row in df.iterrows():
           service=row['fiwareService']
           entityType=row['entityType']
           centroid_lon=row['centroid_location_coordinates_lon']
           centroid_lat=row['centroid_location_coordinates_lat']
           max_distance=row['max_distance']
           totalOfPoints=row['points']
           cluster_id=row['cluster']
           import geopy.distance
           import numpy as np

           if(np.isnan(centroid_lon) or np.isnan(centroid_lat)):
                debugInfo(f"Ignore {service} with centroid ({centroid_lon},{centroid_lat}) in case of {coord_point}")
           else:
                p1 = (centroid_lon,centroid_lat)
                p2 = (longitude,latitude)
                distance=geopy.distance.geodesic(p1,p2).km
                debugDebug(f"Distance of {service}'s centroid {p1}  to the local {p2}:   {distance} kms")
                if(distance<max_distance):
                    debugInfo(f"The Service {service} will be used to analyse data ({totalOfPoints} points) about {entityType}")
                    if(service in services):
                       debugDebug('The service is already resgistered')
                    else:
                       services[service]={}
                    if(entityType in services[service]):
                       debugDebug('The entity is already resgistered in service')
                    else:
                       services[service][entityType]={}
                    services[service][entityType][cluster_id]={'centroid_location_coordinates_lon':centroid_lon,'centroid_location_coordinates_lat':centroid_lat,'max_distance':max_distance,'points':totalOfPoints};
                else:
                    debugInfo(f"The Service {service} will not be used because the {distance} is greater then {max_distance}")
      debugInfo(f"End:Check coords {coord_point}")
      p=p+1
    return services

def find_services(p_list_of_entities,p_list_of_locals,p_list_of_coords):
    df=get_geometadata(p_list_of_entities)
    list_of_services=get_filtered_services(df,p_list_of_locals,p_list_of_coords)
    return list_of_services

def do_test():
    list_of_entities=['TemperatureSensor', 'AirQualityObserved']
    list_of_locals=['São Pedro de Fins, Maia','Matosinhos', 'Vila do Conde']
    list_of_coords=['41.2512, -8.5864','41.1821,-8.6891', '41.3525,-8.7435']

    list_of_services=find_services(list_of_entities,list_of_locals,list_of_coords)
    print(f"Services to use: ")
    import json
    print(json.dumps(list_of_services, sort_keys=False, indent=2))

#do_test()
