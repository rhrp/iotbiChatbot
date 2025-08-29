from utils import debugInfo,debugDebug,DEFAULT_COLLECTION,COLLECTION_ENTITIES,saveInMemory
from config import HTTP_HADOOP_ENDPOINT,HADOOP_USER

def get_vector_store_entities():
        tmp_file="/tmp/entities_1.csv"
        from hdfs import InsecureClient
        client = InsecureClient(HTTP_HADOOP_ENDPOINT, user=HADOOP_USER)
        client.download('/system/metadata/entities.csv',tmp_file, n_threads=5,overwrite=True)
        vector_store=saveInMemory(tmp_file,None)
        fnames = client.list('/system/metadata/',status=True)
        for f in fnames:
            fname=f[0]
            type=f[1]['type']
            if (type=='DIRECTORY'):
                tmp_file="/tmp/entity_"+fname+".csv"
                client.download('/system/metadata/'+fname+'/schema.csv',tmp_file, n_threads=5,overwrite=True)
                debugDebug(f'{fname}  Type={type}')
                saveInMemory(tmp_file,vector_store)
        return vector_store

def find_common_elements(arr1, arr2):
    # Convert arrays to sets for faster lookup
    set1 = set(arr1)
    set2 = set(arr2)
    # Find intersection of the sets (common elements)
    common_elements = set1.intersection(set2)
    return list(common_elements)

def get_vector_store_geometadata(p_list_entities):
    from hdfs import InsecureClient
    client = InsecureClient(HTTP_HADOOP_ENDPOINT, user=HADOOP_USER)
    vector_store=None
    tnames = client.list('/data/',status=False)
    for t in tnames:
        if (t in p_list_of_services):
          debugDebug(f'Tenant: {t}')
          enames = client.list('/data/'+t,status=False)
          entitiesToLoad=find_common_elements(enames,p_list_entities)
          for entityName in entitiesToLoad:
             tmp_file="/tmp/entity_geometadata_"+t+'_'+entityName+".csv"
             debugInfo(f'Loading {t}  {entityName}...')
             client.download('/data/'+t+'/'+entityName+'/geometadata.csv',tmp_file, n_threads=5,overwrite=True)
             debugInfo(f'Add to vector store...')
             vector_store=saveInMemory(tmp_file,vector_store)
             debugInfo(f'Next!')
        else:
         debugDebug(f'Tenant: {t} is ignores') 
    return vector_store

def get_vector_store_data_version1(p_list_of_services,p_list_entities):
        from hdfs import InsecureClient
        client = InsecureClient(HTTP_HADOOP_ENDPOINT, user=HADOOP_USER)
        vector_store=None
        tnames = client.list('/data/',status=False)
        for t in tnames:
           if (t in p_list_of_services):
              debugDebug(f'Tenant: {t}')
              enames = client.list('/data/'+t,status=False)
              entitiesToLoad=find_common_elements(enames,p_list_entities)
              for entityName in entitiesToLoad:
                 tmp_file="/tmp/entity_current_"+service+'_'+entityName+".csv"
                 debugInfo(f'Loading {t}  {entityName}...')
                 client.download('/data/'+t+'/'+entityName+'/current.csv',tmp_file, n_threads=5,overwrite=True)
                 debugInfo(f'Add to vector store...')
                 vector_store=saveInMemory(tmp_file,vector_store)
                 debugInfo(f'Next!')
           else:
              debugDebug(f'Tenant: {t} is ignores')
        return vector_store

def get_vector_store_data(p_list_of_services):
        from hdfs import InsecureClient
        client = InsecureClient(HTTP_HADOOP_ENDPOINT, user=HADOOP_USER)
        vector_store=None
        for service in p_list_of_services:
           debugDebug(f'Service: {service}')
           entitiesToLoad=p_list_of_services[service]
           for entityName in entitiesToLoad:
              totalOfPoints=0
              for cluster_id in p_list_of_services[service][entityName]:
                 totalOfPoints=totalOfPoints+p_list_of_services[service][entityName][cluster_id]['points']
              if(totalOfPoints<30):
                 tmp_file="/tmp/entity_current_"+service+'_'+entityName+".csv"
                 debugInfo(f'Loading {service}  {entityName}...')
                 client.download('/data/'+service+'/'+entityName+'/current.csv',tmp_file, n_threads=5,overwrite=True)
                 debugInfo(f'Add to vector store...')
                 vector_store=saveInMemory(tmp_file,vector_store)
                 debugInfo(f'Next!')
              else:
                 debugInfo(f'Service: {service} and Entity {entityName} is ignored due to its size of {totalOfPoints} points')
        return vector_store
