import pyspark 
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("geoBoundariesQAQC_dev").master("yarn").getOrCreate()
  
import os



def install(x):
  import subprocess
  uninst_str = "/usr/bin/pip uninstall " + x
  subprocess.call(["pip3", "uninstall", x])
  inst_str = "pip install " + x
  subprocess.call(["pip3", "install", x])
  import importlib
  test = importlib.import_module(x)
  return x


rdd = spark.sparkContext.parallelize([1,2,3,4,5,6,7]) ## assuming 4 worker nodes
rdd.map(lambda x: install("numpy")).collect() 