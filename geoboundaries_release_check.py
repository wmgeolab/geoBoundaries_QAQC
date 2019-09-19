import importlib
import retrieve_files
import metadata_check
from os import listdir
from os.path import isfile, join
import pandas as pd
import zipfile
import requests
import os
import pyspark 
from pyspark.sql import SparkSession
import hdfs
from hdfs.ext.kerberos import KerberosClient
import uuid
import shutil
from pyspark.sql.functions import udf
import sparkImport
from ftplib import FTP
import fileinput
import time
import sys
import hashlib

importlib.reload(retrieve_files)
importlib.reload(metadata_check)
importlib.reload(sparkImport)

def geoBoundaries_check(boundary_set_name, checkMeta = True, checkBoundary = True, skipDownload = False):
  check_results = {}
  
  #Make sure we're in root...
  os.chdir(os.path.expanduser("~/"))
  
  if(skipDownload == False):
    print("+++++++++++++++++++++++++++++++++++")
    print("+++++++++++++++++++++++++++++++++++")
    print("Downloading " + boundary_set_name + " from Google Drive.")
    print("Downloading Metadata: " + str(checkMeta))
    print("Downloading Boundaries: " + str(checkBoundary))
    print("+++++++++++++++++++++++++++++++++++")
    print("+++++++++++++++++++++++++++++++++++")
    check_results["download"] = retrieve_files.download_from_gDrive(boundary_set_name, checkMeta, checkBoundary)
  else:
    check_results["download"] = "PASSED"
  
  if(check_results["download"] == "PASSED"):
    print("+++++++++++++++++++++++++++++++++++")
    print("+++++++++++++++++++++++++++++++++++")
    print("Downloads complete.")
    print("+++++++++++++++++++++++++++++++++++")
    print("+++++++++++++++++++++++++++++++++++")
        
    if(checkMeta == True):
      print("")
      print("")
      print("+++++++++++++++++++++++++++++++++++")
      print("+++++++++++++++++++++++++++++++++++")
      print("Commencing Metadata Checks.")
      print("+++++++++++++++++++++++++++++++++++")
      print("+++++++++++++++++++++++++++++++++++")

      metaPassBack = metadata_check.metadata_check(boundary_set_name, "./temp/test.txt", 0)
      metaData = metaPassBack[1]
    
    if(checkMeta == False):
      print("")
      print("WARNING: Metadata checks skipped at user request.")
      metaData_folder = ("./temp/gDrive/" + boundary_set_name + "/metadata/")
      metaFiles = [f for f in listdir(metaData_folder) if isfile(join(metaData_folder, f))]
      metaFile = metaFiles[0]
      print("Loading metadata directly from: " + metaData_folder + metaFiles[0])
      metaData = pd.read_excel(metaData_folder + metaFiles[0])
  
  
    if(checkBoundary == True):
      print("")
      print("")
      print("+++++++++++++++++++++++++++++++++++")
      print("+++++++++++++++++++++++++++++++++++")
      print("Commencing Boundary File Checks.")
      print("+++++++++++++++++++++++++++++++++++")
      print("+++++++++++++++++++++++++++++++++++")
      
      df = file_checks(metaData, boundary_set_name)
 
      
      df["Break_Column"] = "|"
      for i in range(0,len(df.index)):
        elements = str(df.iloc[i,1]).split("letters=")
        for j in range(0,len(elements)):
          if(len(elements[j]) > 10):

            c_target = str(hashlib.md5(elements[j].encode()).hexdigest())
            if c_target in df.columns:
              ne=1
            else:
              df[c_target] = ""

            df.loc[i,c_target] = elements[j]

      #Merge all the rest of the checks back in
      cDf = pd.merge(metaData, df, on='Processed File Name')

      cDf.to_csv('/home/cdsw/temp.csv')

      #upload
      ftp_user = "geologs"
      ftp_pass = "$S@Dxa8PRU#Pyze"
      ftp_domain = "eddie.dreamhost.com"

      ftp_object = FTP()
      ftp_object.connect(ftp_domain, 21)
      ftp_object.login(ftp_user, ftp_pass)
      ftp_object.cwd("/logs.geoboundaries.org/build_logs")
      ftp_file_location = '/home/cdsw/temp.csv'

      remote_name = (str(boundary_set_name) 
                     + "_CB_" + str(checkMeta) 
                     + "_CB_" + str(checkBoundary)
                     + "_SD_" + str(skipDownload)
                     + "_TI_" + str(time.strftime("%Y-%m-%d_%H:%M:%S")) + ".csv")

      cp = open(ftp_file_location, 'rb')
      ftp_object.storbinary('STOR %s' % remote_name, cp, 1024)
      cp.close()

      print("All done! You can access the log of this build at logs.geoboundaries.org/build_logs/" + remote_name)
      
      return str("logs.geoboundaries.org/build_logs/" + remote_name)

    
  else:
    print("ERROR: Download failed, I have no files to analyze!")

####################
####################
####################
####################
####################
####################
####################Shape Checks - embedded here
####################Until I figure out spark user defined imports
####################
####################
####################
####################
####################



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
      
  

    
  spark = (SparkSession
           .builder
           .appName("geoBoundariesQAQC_dev")
           .master("yarn")
           .config("spark.executor.cores", 16)
           .config("spark.yarn.executor.cores", 16)
           .config("spark.executor.instances", 16)
           .config("spark.yarn.executor.instances", 16)
           .config("spark.yarn.driver.cores", 16)
#           .config("spark.dynamicAllocation.enabled","true")
#           .config("spark.dynamicAllocation.minExecutors", 32)
#           .config("spark.dynamicAllocation.maxExecutors", 64)
#           .config("spark.yarn.dynamicAllocation.enabled","true")
#           .config("spark.yarn.dynamicAllocation.minExecutors", 32)
#           .config("spark.yarn.dynamicAllocation.maxExecutors", 64)
           .config("spark.yarn.executor.memory",'32g')
           .config("spark.yarn.executor.memoryOverhead",'100g')
           .config("spark.yarn.driver.memoryOverhead",'100g')
           .config("spark.yarn.nodemanager.vmem-check-enabled", 'false')
           .getOrCreate())
  
 
  
  #Build schema for UDF return
  check_shape_schema = StructType([
    StructField('letters', StringType(), nullable=False),
    StructField('letters', StringType(), nullable=False)
  ])

  bndPath = "/projects/geoboundaries/" + boundary_set_name
  spark_df = spark.createDataFrame(metaData[["Processed File Name", "Boundary Level"]])
  spark_df.show()
  
  #test_udf = udf(really_simple, check_shape_schema)
  
  check_shape_udf = udf(lambda x: check_shapefiles(bndPath, x), check_shape_schema)
  
  output = spark_df.select("Processed File Name", check_shape_udf("Processed File Name").alias("Raw File Check Log"))
  
  output.show()
  
  #Upload the resultant CSV
  df = output.toPandas()
  
  return df
  
  
  

  
def check_shapefiles(path_to_zip, zipID):
  import os
  import numpy
  import hdfs
  import subprocess
  from hdfs.ext.kerberos import KerberosClient
  import pandas as pd
  import geopandas
  import fiona
  from shapely.geometry import mapping, shape
  
  try:
    subprocess.call(["kinit", "dsmillerrunfol@CAMPUS.WM.EDU","-k","-t","/home/dsmillerrunfol@campus.wm.edu/danr.keytab"])
    hdfs_host = "https://m1a.geo.sciclone.wm.edu:14000"

    client_hdfs = KerberosClient(hdfs_host)

    ret_obj = {}

    t_id = str(uuid.uuid4())
    try:
      os.mkdir("./temp")
    except:
      1 == 1
    
    
    localDest = "./temp/" + t_id + ".zip"
    client_hdfs.download(path_to_zip + "/" + zipID, localDest, overwrite=True)
    ret_obj["error"] = ""
    ret_obj["warning"] = ""  

    if(zipfile.is_zipfile(localDest)):

      with zipfile.ZipFile(localDest) as zipObj:
        zipObj.extractall("./tempSparks/" + t_id + "/")
    else:
      ret_obj["error"] = "The zip file failed to extract."
      return [ret_obj["error"], ret_obj["warning"]]


    
  except:
      ret_obj["error"] = "Something went really, really wrong here.  Probably a corrupt zip, but could be lots of things... |"
      return [ret_obj["error"], ret_obj["warning"]]
    
    #Ok!  File is now extracted and available at path locally:
               #"./tempSparks/" + t_id + "/"

    #Now let's load the metadata for this file in...:
  
  
  #Let's actually do some file checks, recording any errors we find to the appropriate dataframe row.
  try:
    #Load into geopands
    shp = geopandas.read_file("./tempSparks/" + t_id + "/")

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
          ret_obj["warning"] = "There is a minor issue with this boundary - i.e., a river might cross somewhere it should not.  We can fix it automatically by using a buffer of 0 in shapely, but this message indicates you should look carefully at the file sometime soon."
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
  except:
    ret_obj["error"] = "Something bad happened while we were trying to unpack the shapefile.  This error indicates it wasn't a specific issue, but rather the entire shapefile seems corrupted (or something equally bad!)"
    return [ret_obj["error"], ret_obj["warning"]]

geoBoundaries_check(boundary_set_name = os.environ["gb_version"])
    
#geoBoundaries_check(boundary_set_name = "gbReleaseCandidate_0_0_0_3", 
#                    checkMeta = True, checkBoundary = True, skipDownload= False)
