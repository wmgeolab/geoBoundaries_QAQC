import zipfile
import pandas as pd
import requests
import os
import pyspark 
from pyspark.sql import SparkSession
import hdfs
from hdfs.ext.kerberos import KerberosClient
import uuid
import shutil

hdfs_host = "https://m1a.geo.sciclone.wm.edu"
hdfs_port = "14000"
client_hdfs = KerberosClient(str(hdfs_host)+":"+str(hdfs_port))


def file_checks(metaData, boundary_set_name):
  
  total_count = len(metaData.index)
  file_validity = 1
  
  to_path = "/projects/geoboundaries/" + boundary_set_name + "/"
  client_hdfs.delete(to_path, recursive=True)
  
  for index, row in metaData.iterrows():
    print("Processing File and Checking Boundary (" +  str(index+1) +" | " + str(total_count) + ") " + row["Processed File Name"])
    
    #Copy zipfiles to HDFS
    try:
      expected_path = ('/gDrive/' + boundary_set_name + "/" + 
                       row["Boundary Level"].replace(" ", "") + "/" + 
                       row["Processed File Name"])
        
      path_to_extracted_file = os.path.abspath("./temp") + expected_path
      #Copy the zip file to HDFS
      from_path = path_to_extracted_file
      
      with client_hdfs.write(to_path+row["Processed File Name"]) as writer, open(from_path, 'rb') as reader:
        shutil.copyfileobj(reader, writer)

    except Exception as e:
      file_validity = 0
      metaData.loc[index, "Zip File Validity Check"] = "Something is wrong with the zip file - it failed to unzip."
      print(e)
      
  
  
  #spark = SparkSession.builder.appName("LocalSparkSession").master("yarn").config("spark.yarn.security.tokens.hbase.enabled", "false").getOrCreate()
  #spark = SparkSession.builder.appName("LocalSparkSession").master("yarn").config("spark.yarn.security.tokens.hdfs.enabled", "true").getOrCreate()
  

  #spark_df = spark.createDataFrame(metaData[["Processed File Name", "Boundary Level"]])
  #spark_df.show()
  
  check_shapefiles(client_hdfs, ("/projects/geoboundaries/" + boundary_set_name + "/FRA_ADM0.zip"))
  
  
  #spark.stop()
  


def check_shapefiles(client_hdfs, path_to_zip):
  t_id = str(uuid.uuid4())
  try:
    os.mkdir("./temp)
  except:
    1 == 1
  localDest = "./temp/" + t_id + ".zip"
  print(path_to_zip)
  print(localDest)
  client_hdfs.download(path_to_zip, localDest, overwrite=True)
  with zipfile.ZipFile(localDest) as zipObj:
    zipObj.extractall("./tempSparks/" + t_id + "/")
  
  #Ok!  File is now extracted and available at path locally:
             #"./tempSparks/" + t_id + "/"
  #Let's actually do some file checks.
  
  
#client_hdfs.upload(local_path=srcPath, hdfs_path=destPath)
      




