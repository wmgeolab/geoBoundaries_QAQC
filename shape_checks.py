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


#Imports for spatial checks
import geopandas
from shapely.geometry import mapping, shape
from pyspark.sql.types import StructType, StructField, StringType

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
      
  

    
  spark = SparkSession.builder.appName("geoBoundariesQAQC_dev").master("yarn").config("spark.yarn.security.tokens.hdfs.enabled", "true").getOrCreate()
  
  
  #Build schema for UDF return
  check_shape_schema = StructType([
    StructField('letters', StringType(), nullable=False),
    StructField('letters', StringType(), nullable=False)
  ])

  bndPath = "/projects/geoboundaries/" + boundary_set_name
  spark_df = spark.createDataFrame(metaData[["Processed File Name", "Boundary Level"]])
  spark_df.show()
  
  
  spark.udf.register('testing', lambda zipID: really_simple(zipID), check_shape_schema)
  
  #spark.udf.register('first_udf', lambda zipID: check_shapefiles(client_hdfs,bndPath,zipID), check_shape_schema)
  
  
  spark.sql("select testing('Processed File Name') as value").show()
  #spark_df.select('Processed File Name', spark_convert_ascii('Processed File Name').alias('ascii_map'))
  
  #spark_df.show()
  
  #check_shapefiles(client_hdfs, ("/projects/geoboundaries/" + boundary_set_name), "FRA_ADM0")
  
  
  spark.stop()
  


def really_simple(test):
  return [test, test]
  
def check_shapefiles(client_hdfs, path_to_zip, zipID):
  
  ret_obj = {}
  
  t_id = str(uuid.uuid4())
  try:
    os.mkdir("./temp")
  except:
    1 == 1
  localDest = "./temp/" + t_id + ".zip"
  client_hdfs.download(path_to_zip + "/" + zipID + ".zip", localDest, overwrite=True)
  with zipfile.ZipFile(localDest) as zipObj:
    zipObj.extractall("./tempSparks/" + t_id + "/")
  
  #Ok!  File is now extracted and available at path locally:
             #"./tempSparks/" + t_id + "/"
    
  #Now let's load the metadata for this file in...:
  
  
  #Let's actually do some file checks, recording any errors we find to the appropriate dataframe row.
  
  #Load into geopands
  shp = geopandas.read_file("./tempSparks/" + t_id + "/")
  ret_obj["error"] = ""
  ret_obj["warning"] = ""
  if(not (shp.crs["init"] == "epsg:4326")):
    ret_obj["error"] = ret_obj["error"] + 'Projection was incorrect.  EPSG must be 4326. | '
  tol = 1e-12
  xmin = shp.bounds["minx"].values[0]
  ymin = shp.bounds["miny"].values[0]
  xmax = shp.bounds["maxx"].values[0]
  ymax = shp.bounds["maxy"].values[0]
  
  valid = ((xmin >= -180-tol) and (xmax <= 180+tol) and (ymin >= -90-tol) and (ymax <= 90+tol))
  if not valid:
    ret_obj["error"] = ret_obj["error"] + "Bounds appear to be in another castle. xmin: {0}, xmax: {1}, ymin: {2}, ymax: {3} | ".format(xmin, xmax, ymin, ymax)
  
  #Fiona checks
  shp = fiona.open("./tempSparks/" + t_id + "/")
  valid = True
  error = None
  fixed = []
  for feature in shp:
    raw_shape = shape(feature['geometry'])
    valid = raw_shape.is_valid
    if valid:
      fixed.append(feature)
    if not valid:
      fixed_shape = raw_shape.buffer(0)
      fix_valid = fixed_shape.is_valid
      if fix_valid and error is None:
        ret_obj["warning"] = "There is a minor issue with this boundary.  We can fix it automatically, but this message indicates you should look carefully at the file sometime soon."
        feature["geometry"] = mapping(fixed_shape)
        fixed.append(feature)
      elif not fix_valid:
        if error is not None:
          ret_obj["error"] = ret_obj["error"] + "An error in the geometry of the file exists that we could not automatically fix. | "
        else:
          ret_obj["error"] = ret_obj["error"] + "A really bad error in the geometry of the file exists that we could not automatically fix. | "
        break
  
  
  #Clean up temp files
  shutil.rmtree("./tempSparks/" + t_id + "/", ignore_errors=True)
  os.remove("./temp/" + t_id + ".zip")
  
  return [ret_obj["error"], ret_obj["warning"]]
    
    
  
  
    
  
  
#client_hdfs.upload(local_path=srcPath, hdfs_path=destPath)
      




