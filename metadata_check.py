import pandas as pd
from os import listdir
from os.path import isfile, join
from pathlib import Path
import requests
import os


def metadata_check(boundary_set_name, metaQALogPath, version_number):
  pass_flag = 1
  
  print("Searching for file " + str(boundary_set_name) + ".xlsx (note google drive automatically converts sheets to XLSX when downloaded).")
  metaData_folder = ("./temp/gDrive/" + boundary_set_name + "/metadata/") 
  metaFiles = [f for f in listdir(metaData_folder) if isfile(join(metaData_folder, f))]
  if(len(metaFiles) > 0):
    print("Metadata file found - testing it now.")
    print(metaData_folder + metaFiles[0])
    
  metaData = pd.read_excel(metaData_folder + metaFiles[0])   
  
  #Load in valid country names and ISO codes
  iso_name_Dta = pd.read_csv("./ISOs.csv")  
    
  files_exist_pass = 1
  name_pass = 1
  iso_pass = 1
  source_pass = 1
  license_pass = 1
  link_pass = 1
  
  total_count = len(metaData.index)
  
  #Load in a list of all existant zipfiles in the version
  posix_existing_paths = list(Path("./temp/gDrive/" + boundary_set_name + "/").rglob("*"))
  existing_paths = [os.fspath(item) for item in posix_existing_paths]
  
  
  for index, row in metaData.iterrows():
    print("Processing Metadata (" +  str(index) +" | " + str(total_count) + ") " + row["Processed File Name"])
    #Check the file name is valid and exists in the zipfile
    length = len(str(row["Processed File Name"]))

    if(length != 12):
      metaData.loc[index, "Process File Name QA Check"] = "The processed file name has too many characters.  It needs to be of the format 'XXX_YYY.ZIP'."
      pass_flag = 0
      name_pass = 0 
    else:
      metaData.loc[index, "Process File Name QA Check"] = ""
      
    ADM_code = str(row["Processed File Name"])[:3]
    if(not ADM_code in iso_name_Dta["ISO-alpha3 code"].values):
      pass_flag = 0
      iso_pass = 0
      metaData.loc[index, "File Name ISO Code"] = ("The first three characters of the file"+
                                                   "are not a valid ISO code." + (ADM_code))

    else:
      metaData.loc[index, "File Name ISO Code"] = ""
    
    
    #Check file reference exists
    expected_path = ("temp/gDrive/" + boundary_set_name + "/" + 
                     row["Boundary Level"].replace(" ", "") + "/" + 
                    row["Processed File Name"])

    if(not expected_path in existing_paths):
      pass_flag = 0
      files_exist_pass = 0
      metaData.loc[index, "Process File Name Existance Check"] = "This file does not exist.  File should be at /ADM0/XXX_YYY.zip." 
    else:
      metaData.loc[index, "Process File Name Existance Check"] = ""
    
    if(pd.isnull(metaData.loc[index]["Source 1"]) and pd.isnull(metaData.loc[index]["Source 2"])):
      pass_flag = 0
      source_pass = 0
      metaData.loc[index, "Source Check"] = "You must have at least one source."
    else:
      metaData.loc[index, "Source Check"] = ""
    
    
    acceptable_licenses = ["Public Domain",
                           "Open Data Commons Open Database License 1.0",
                           "Creative Commons Attribution-ShareAlike 2.0",
                           "Creative Commons Attribution-ShareAlike 3.0",
                           "Creative Commons Attribution 3.0 IGO",
                           "Creative Commons Attribution 4.0 International",
                           "Creative Commons Attribution - NonCommercial 4.0 International (CC BY-NC 4.0)",
                           "Other"]
    
    #Check the license field matches one of the acceptable licenses.
    if(not (row["License"] in acceptable_licenses)):
      metaData.loc[index, "License Check"] = ("The license is not on the list of " +
                                              "acceptable licenses.  Check the " + 
                                              "geolab.wm.edu/data geoBoundaries " + 
                                              "page for the most recent list of " + 
                                              "acceptable license types.  You may " + 
                                              "also use 'Other' and place the license " + 
                                              "details in the license details column.")
      license_pass = 0
      pass_flag = 0
    
    else:
      metaData.loc[index, "License Check"] = ""
    
    
    #Check if the license type is "Other" that ther eis a license detail.
    if(row["License"] == "Other"):
      if(pd.isnull(row["License Detail"])):
        license_pass = 0
        pass_flag = 0
        metaData.loc[index, "License Detail Check"] = ("If license type is other, " +
                                                       "you must enter the license " +
                                                       "detail.")
      elif(len(row["License Detail"]) < 1):
        license_pass = 0
        pass_flag = 0
        metaData.loc[index, "License Detail Check"] = ("If license type is other, " +
                                                       "you must enter the license " +
                                                       "detail.")
        
    #Check the license source exists *and can load*.  Must be a resolvable URL.
    #Students should create a tweet with relevant license data to cite if
    #an informal source / screenshot etc.
    
    if(pd.isnull(row["License Source"])):
      license_pass = 0
      pass_flag = 0
      metaData.loc[index, "License Source Check"] = ("You must enter a license source. " +
                                                     "Further, the source must be a URL "+
                                                     "and that URL must work today. " +
                                                     "If there is no URL, for example "+
                                                     "if the website is down or it " +
                                                     "was personal communications, " +
                                                     "use the geoLab twitter via Slack " +
                                                     "to create a tweet with the appropriate " +
                                                     "screenshot, along with the message " +
                                                     "Thanks to _____ for making their data public!" +
                                                     "and use the URL to that tweet as the License Source."
                                                    )
    elif(len(row["License Source"]) < 1):
      license_pass = 0
      pass_flag = 0
      metaData.loc[index, "License Source Check"] = ("You must enter a license source. " +
                                                     "Further, the source must be a URL "+
                                                     "and that URL must work today. " +
                                                     "If there is no URL, for example "+
                                                     "if the website is down or it " +
                                                     "was personal communications, " +
                                                     "use the geoLab twitter via Slack " +
                                                     "to create a tweet with the appropriate " +
                                                     "screenshot, along with the message " +
                                                     "Thanks to _____ for making their data public!" +
                                                     "and use the URL to that tweet as the License Source."
                                                    )
      
      
  #Check source license URL exists.
    elif(not str(requests.get(row["License Source"])) == '<Response [200]>'):
      license_pass = 0
      pass_flag = 0
      metaData.loc[index, "License Source Check"] = ("You must enter a license source. " +
                                                     "Further, the source must be a URL "+
                                                     "and that URL must work today. " +
                                                     "If there is no URL, for example "+
                                                     "if the website is down or it " +
                                                     "was personal communications, " +
                                                     "use the geoLab twitter via Slack " +
                                                     "to create a tweet with the appropriate " +
                                                     "screenshot, along with the message " +
                                                     "Thanks to _____ for making their data public!" +
                                                     "and use the URL to that tweet as the License Source."
                                                    )
  
    else:
      metaData.loc[index, "License Source Check"] = ""
     
     
    if(pd.isnull(row["Link to Source Data"])):
      link_pass = 0
      pass_flag = 0
      metaData.loc[index, "Link to Source Data Check"] = ("You must enter a data source. " +
                                                     "Further, the source must be a URL "+
                                                     "and that URL must work today. " +
                                                     "If there is no URL, for example "+
                                                     "if the website is down or it " +
                                                     "was personal communications, " +
                                                     "use the geoLab twitter via Slack " +
                                                     "to create a tweet with the appropriate " +
                                                     "screenshot, along with the message " +
                                                     "Thanks to _____ for making their data public!" +
                                                     "and use the URL to that tweet as the data source."
                                                    )
    elif(len(row["Link to Source Data"]) < 1):
      link_pass = 0
      pass_flag = 0
      metaData.loc[index, "Link to Source Data Check"] = ("You must enter a data source. " +
                                                     "Further, the source must be a URL "+
                                                     "and that URL must work today. " +
                                                     "If there is no URL, for example "+
                                                     "if the website is down or it " +
                                                     "was personal communications, " +
                                                     "use the geoLab twitter via Slack " +
                                                     "to create a tweet with the appropriate " +
                                                     "screenshot, along with the message " +
                                                     "Thanks to _____ for making their data public!" +
                                                     "and use the URL to that tweet as the data source."
                                                    )
      
      
  #Check source license URL exists.
    elif(not str(requests.get(row["Link to Source Data"])) == '<Response [200]>'):
      link_pass = 0
      pass_flag = 0
      metaData.loc[index, "Link to Source Data Check"] = ("You must enter a license source. " +
                                                     "Further, the source must be a URL "+
                                                     "and that URL must work today. " +
                                                     "If there is no URL, for example "+
                                                     "if the website is down or it " +
                                                     "was personal communications, " +
                                                     "use the geoLab twitter via Slack " +
                                                     "to create a tweet with the appropriate " +
                                                     "screenshot, along with the message " +
                                                     "Thanks to _____ for making their data public!" +
                                                     "and use the URL to that tweet as the License Source."
                                                    )
  
    else:
      metaData.loc[index, "Link to Source Data Check"] = ""
    
    

  #Output to terminal.
  print("")
  if(name_pass == 0):
      print("All processed file names correct length................... FAILED") 
  else:
      print("All processed file names correct length................... PASSED") 
  
  if(iso_pass == 0):
      print("All processed file names correct ISO...................... FAILED") 
  else:
      print("All processed file names correct ISO...................... PASSED") 
  
  if(files_exist_pass == 0):
      print("All processed file names exist............................ FAILED") 
  else:
      print("All processed file names exist............................ PASSED") 
  
  if(source_pass == 0):
      print("All processed files have a source......................... FAILED") 
  else:
      print("All processed files have a source......................... PASSED")
    
  
  if(license_pass == 0):
      print("All processed files have a valid license with link........ FAILED") 
  else:
      print("All processed files have a valid license with link........ PASSED")
     
     
  if(link_pass == 0):
      print("All processed files have a source link.................... FAILED") 
  else:
      print("All processed files have a source link.................... PASSED")
      
  return [pass_flag, metaData]