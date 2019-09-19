import os

def sparkImport(package, local_dir = "~/sparkImportDependencies/", python_version = "3.6", force_install = False):

  local_dir = (os.path.expanduser(local_dir))
  #First, build the url to download the package from:
  first_letter = package[0][0]
  ##print(first_letter)
  url = ("https://pypi.io/packages/source/" + package[0][0] + "/" + package[0] +
        "/" + package[0] + "-" + str(package[1]) + ".tar.gz" ) 
  urlZip = ("https://pypi.io/packages/source/" + package[0][0] + "/" + package[0] +
        "/" + package[0] + "-" + str(package[1]) + ".zip" ) 
  
  depPath = local_dir + package[0] + "-" + str(package[1]) + ".tar.gz"
  depPathZip = local_dir + package[0] + "-" + str(package[1]) + ".zip"
  
  ##print(depPath)
  if(os.path.isfile(os.path.abspath(depPath)) or os.path.isfile(os.path.abspath(depPathZip))):
    #print("Package already downloaded for sparkImport.  Skipping wget.")
    1==1
  
  else:
    os.system("wget -P " + local_dir + " " + url)
    os.system("wget -P " + local_dir + " " + urlZip)
    
  #Verify download success:
  if(os.path.isfile(os.path.abspath(depPath)) or os.path.isfile(os.path.abspath(depPathZip))):
    
    #Confirm if the egg already exists - skip if it does.
    #Note, for python packages with a "-", need to replace with "_" for egg.
    egg_name = (package[0].replace("-", "_") + "-" + str(package[1]) +
                "-py" + str(python_version) + ".egg")
    
    if(os.path.isfile(os.path.abspath(local_dir + egg_name)) and force_install == False):
      ##print("Egg already exists - moving to next package.  If you want to force installations (including dependencies), set force_install to True.")
      return(local_dir + egg_name)
    
    else:
      
      #Confirm if it's a zip or a tar
      if(os.path.isfile(os.path.abspath(depPathZip))):
        modPath = os.path.abspath(depPathZip)
        #untar file.  Delete any older folder by the same exact name.
        target_folder = os.path.abspath(local_dir + package[0] + "-" + str(package[1]) +"/")
        os.system("rm -r " + target_folder)
        os.system("unzip " + os.path.abspath(depPathZip) + " -d " +  os.path.abspath(local_dir))
      
      if(os.path.isfile(os.path.abspath(depPath))):
        modPath = os.path.abspath(depPath)
        #untar file.  Delete any older folder by the same exact name.
        target_folder = os.path.abspath(local_dir + package[0] + "-" + str(package[1]) +"/")
        os.system("rm -r " + target_folder)
        os.system("tar xvzf " + os.path.abspath(depPath) + " -C " +  os.path.abspath(local_dir))

      #Build the egg
      cmd = "python3 " + target_folder + "/setup.py bdist_egg"

      #Hacky solution - not sure why building eggs requires you to be in the
      #same directory as the setup.py is, but more googling will happen one day.
      os.chdir(target_folder)
      os.system(cmd)

      #Copy the egg back to the root for distribution later.
      os.chdir("/")

      target_folder = os.path.abspath(local_dir + package[0] + "-" + str(package[1]) +"/")
      
      #Copy the egg:
      cp_cmd = "cp " + target_folder + "/dist/" +  egg_name + " " + local_dir + egg_name
          
      

      if(os.system(cp_cmd) == 0):
        return(local_dir + egg_name)
    
      else:
        #Don't ask.  
        egg_name = (package[0].replace("-", "_") + "-" + str(package[1]) +
                  "-py" + str(python_version) + "-linux-x86_64.egg")
        egg_name_cp = (package[0].replace("-", "_") + "-" + str(package[1]) +
                "-py" + str(python_version) + ".egg")
        cp_cmd = "cp " + target_folder + "/dist/" +  egg_name + " " + local_dir + egg_name_cp
        os.system(cp_cmd)
        egg_name = (package[0].replace("-", "_") + "-" + str(package[1]) +
                  "-py" + str(python_version) + ".egg")
        
    return(local_dir + egg_name)



      
      
     
    
    
    
  else:
    print("Something went wrong with the module download.  Double check it exists" +
          " on pypi, and you specified the version number correctly.")
      
  
  
  #spark.sparkContext.addPyFile("/home/cdsw/dependencies/hdfs.egg")
  
  

sparkImport(["hdfs", "2.5.8"])