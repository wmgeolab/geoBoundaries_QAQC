import importlib
import retrieve_files
import metadata_check
from os import listdir
from os.path import isfile, join
import pandas as pd
import shape_checks

importlib.reload(retrieve_files)
importlib.reload(metadata_check)
importlib.reload(shape_checks)

def geoBoundaries_check(boundary_set_name, checkMeta, checkBoundary):
  check_results = {}
  print("+++++++++++++++++++++++++++++++++++")
  print("+++++++++++++++++++++++++++++++++++")
  print("Downloading " + boundary_set_name + " from Google Drive.")
  print("Downloading Metadata: " + str(checkMeta))
  print("Downloading Boundaries: " + str(checkBoundary))
  print("+++++++++++++++++++++++++++++++++++")
  print("+++++++++++++++++++++++++++++++++++")
  check_results["download"] = retrieve_files.download_from_gDrive(boundary_set_name, checkMeta, checkBoundary)
  
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
      
      shape_checks.file_checks(metaData, boundary_set_name)
 
      
    
  else:
    print("ERROR: Download failed, I have no files to analyze!")
      
    
  
geoBoundaries_check(boundary_set_name = "gbReleaseCandidate_0_0_0_2", 
                    checkMeta = False, checkBoundary = True)

#      metaDataQAQC[1].to_csv(metaQALogPath, header=True)