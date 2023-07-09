
import os
import uuid
import sys
import requests
import datetime
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
 

def get_dbutils():
    try:
        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
    except ImportError:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils

dbutils = get_dbutils()

# Get the file from http server
def get_file_from_url(url, name, tmp_folder):
    # local_path = os.path.expanduser(name)
    if not os.path.exists(f"/dbfs{tmp_folder}/{name}"):
        os.makedirs(f"/dbfs{tmp_folder}/{name}")
        
    filename = url.split('/')[-1]  
    print(f'Getting  {url}')
        
    r = requests.get(url)
    if r.status_code == 200:
        filepath = f'/dbfs{tmp_folder}/{name}/{filename}'
        open(filepath, 'wb').write(r.content) 
        return filepath
    else:
        raise Exception(f"{filename} has response {r.status_code}") 

# Get the file from http server
def get_files(target_files, tmp_folder):

    filepaths = {}

    try:
        now     = datetime.datetime.now()
        year    = now.year
        month   = now.month
        day     = now.day
        hour    = now.hour
        minute  = now.minute

        for name in target_files:
            url = target_files[name]
            new_url = url.replace('{{YYYY}}', f'{year:04d}') \
                            .replace('{{MM}}', f"{month:02d}") \
                            .replace('{{DD}}', f"{day:02d}") \
                            .replace('{{HH}}', f"{hour:02d}")
    
            # print(f"   url is {new_url}") 
            filepaths[name] = get_file_from_url(new_url, name, tmp_folder) 
            # print(f"   file location is is {file_location}")                        
 
        return filepaths

    except Exception as e:
        print(e)    

# Upload the file to a cloud storage location
def upload_files(filepaths, container_root_path):
    try:
        for name in filepaths:
            file_path = filepaths[name].replace("/dbfs", "")
            filename = filepaths[name].split('/')[-1]
            container_path = f"{container_root_path}/{name}/{filename}" 
            print(f"Uploading {filename} to {container_path}")
            dbutils.fs.cp(file_path, container_path)

    except Exception as e:
        print(e)             