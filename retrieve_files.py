#!/usr/bin/python
from __future__ import print_function
import sys
import httplib2
import os
import datetime
import io
import socket
from time import gmtime, strftime
from apiclient import discovery
import oauth2client
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
from datetime import date
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
import shutil

#########################################################################
# Made for Python 3.5 and Google Drive API v3.
# Usage: to replicate a Gogle Drive folder structure with all files, in your computer
# python dl_gdrive_folder.py <Google drive folder source> <local computer folder destination>
# Example:
# Folder Structure at Google Drive:
# F1
#	File0
#	F11
#		File1
#	F12
#		F211
#			FileA
#			FileB
# To download everything from folder F211 to a local folder TST
# python dl_gdrive_folder.py F211 TST
# It doesn´t work if you have more than one folder with the same name
#########################################################################
# Build from the works of :
# Mark Culhane - google_drive_backup.py
# https://github.com/markz0r/tools/blob/master/backup_scripts/google_drive_backup.py
# and
# HatsuneMiku - recursive navigation over folders
# http://stackoverflow.com/questions/22092402/python-google-drive-api-list-the-entire-drive-file-tree
# https://github.com/HatsuneMiku/googleDriveAccess
#
# Know Issues:
#		- wasn´t downloading files if verbose was not used - fixed on 19/01/2017
#		- don´t work with Google "Forms" - fixed (simply skip those files)
#		- downloads trashed files - fixed (don´t even consider them)
#########################################################################

#########################################################################
# Pre-requisites - Authorize API usage on google drive, and download/install google drive pyhton api.
# Follow instructions on : https://developers.google.com/drive/v3/web/quickstart/python
# To install python client on windows:
# pip install --upgrade google-api-python-client
# Authorize API https://developers.google.com/drive/v3/web/quickstart/python#step_1_turn_on_the_api_name
# Download the client_secret.json to same dir as this script
#########################################################################


CLIENT_SECRET_FILE = 'client_secret.json'
SCOPES = ['https://www.googleapis.com/auth/drive']
APPLICATION_NAME = 'Drive File API - Python'
FOLDER_TYPE = 'application/vnd.google-apps.folder'

to_dir = "./temp/gDrive"
from_dir = ""
listing = False
verbose = False
overwrite = True

global num_files
global num_skiped
global last_num 
last_num = 0

def get_credentials():
  creds = None
  # The file token.pickle stores the user's access and refresh tokens, and is
  # created automatically when the authorization flow completes for the first
  # time.
  if os.path.exists('token.pickle'):
      with open('token.pickle', 'rb') as token:
          creds = pickle.load(token)
  # If there are no (valid) credentials available, let the user log in.
  if not creds or not creds.valid:
      if creds and creds.expired and creds.refresh_token:
          creds.refresh(Request())
      else:
          flow = InstalledAppFlow.from_client_secrets_file(
              'client_secret.json', SCOPES)
          creds = flow.run_local_server(port=0)
      # Save the credentials for the next run
      with open('token.pickle', 'wb') as token:
          pickle.dump(creds, token)
  return creds


def prepDest(folder, spaces):
    if not os.path.exists(folder):
        if listing:
            print("{} Will create folder: {}".format(spaces, folder))
        else:
            if verbose:
                print("{} Creating folder: {}".format(spaces, folder))
            os.makedirs(folder)
            return True
    else:
        #print("{} Folder {} already exists".format(spaces, folder))
        if(overwrite == True):
          #print("Overwrite is set to true - overwriting old data.")
          shutil.rmtree(folder)
          os.makedirs(folder)
          return True
        else:
          print("Overwrite is set to False, so the script will now exit.")
        
    return False


def downloadFile(service, spaces, file_name, file_id, mimeType, dest_folder):
    # Function that performs the download of each file to the specified local folder
    global num_skiped
    valid = True
    if (listing):
        print("{} downloading file: {}, to folder {} \n".format(
            spaces, file_name, dest_folder))
    else:
        if verbose:
            print("{} downloading file: {}, to folder {} \n".format(
                spaces, file_name, dest_folder))
        request = service.files().get_media(fileId=file_id)
        if "application/vnd.google-apps" in mimeType:
            if verbose:
                print("Google apps media types will be exported accordingly")
            if "form" in mimeType:
                print(
                    "Google app Form: {} - cannot be downloaded. Skiping...".format(file_name))
                valid = False
                num_skiped += 1
            elif "document" in mimeType:
                request = service.files().export_media(fileId=file_id,
                                                       mimeType='application/vnd.openxmlformats-officedocument.wordprocessingml.document')
                file_name = file_name + ".docx"
            elif "spreadsheet" in mimeType:
                request = service.files().export_media(fileId=file_id,
                                                       mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                file_name = file_name + ".xlsx"
            elif "presentation" in mimeType:
                request = service.files().export_media(fileId=file_id,
                                                       mimeType='application/vnd.openxmlformats-officedocument.presentationml.presentation')
                file_name = file_name + ".pptx"
            else:
                request = service.files().export_media(
                    fileId=file_id, mimeType='application/pdf')
                file_name = file_name + ".pdf"
        if valid:
            #print("{}Downloading -- {}".format(spaces, file_name))
            response = request.execute()
            with open(os.path.join(dest_folder, file_name), "wb") as wer:
                if verbose:
                    print("Writing file {} to folder {}.\n".format(
                        file_name, dest_folder))
                wer.write(response)
                global num_files
                num_files += 1


def getFolderId(service, folderName):
    query = "name contains '%s' and mimeType = '%s'" % (
        folderName, FOLDER_TYPE)
    fid = None
    if verbose:
        print("Checking source folder existence: {}".format(folderName))
    result = service.files().list(q=query,
                                  pageSize=10, pageToken='', fields="nextPageToken,files(parents,id,name,mimeType)").execute()
    if verbose:
        print("Result: {}".format(result))
    if len(result['files']) == 0:
        print("Folder not found")
    else:
        folder = result.get('files')[0]
        fid = folder['id']
        if verbose:
            print("Found {} folders with this name".format(
                len(result.get('files'))))
            print("Found folder! Id: {}".format(fid))
            print("Name: {}".format(folder['name']))
            for p in folder['parents']:
                print("Parent : {}".format(p))
    return(fid)


def getlist(ds, q, **kwargs):
    result = None
    npt = ''
    while not npt is None:
        if npt != '':
            kwargs['pageToken'] = npt
        entries = ds.files().list(q=q, **kwargs).execute()
        if result is None:
            result = entries
        else:
            result['files'] += entries['files']
        npt = entries.get('nextPageToken')
    return result


def getFolderFiles(service, folderId, folderName, dest_folder, depth, checkMeta, checkBoundary):
    # recursive function that walks down the folder tree creating the local folders and downloading the files
    global last_num
    spaces = ' ' * depth
    d_folder = dest_folder + "" + folderName
    prepDest(d_folder, spaces)
    if(num_files > last_num):
      print("{} files downloaded so far\n".format(num_files))
      last_num = num_files
    if (listing or verbose):
        print("{}+{}\n{}     {}\n".format(spaces, folderId, spaces, folderName))
    else:
        1==1
        print("Analyzing and Downloading From Source Folder: {}\n".format(folderName))

    # searching only for folders
    query = "'%s' in parents and mimeType='%s' and trashed = false" % (
        folderId, FOLDER_TYPE)
    entries = getlist(service, query, **{'pageSize': 1000})
    for folder in entries['files']:
        getFolderFiles(service, folder['id'],
                       folder['name'], d_folder, depth+1, checkMeta, checkBoundary)

    # searching only for files (notice que query is mimTye != FOLDER_TYPE
    query = "'%s' in parents and mimeType!='%s' and trashed = false" % (
        folderId, FOLDER_TYPE)
    entries = getlist(service, query, **{'pageSize': 1000})
    for f in entries['files']:
        if(checkBoundary == True):
          if (listing or verbose):
              print("{} -ID: {} NAME: {} TYPE: {}".format(spaces,
                                                          f['id'], f['name'], f['mimeType']))
          downloadFile(service, spaces, f['name'],
                       f['id'], f['mimeType'], d_folder)
        else:
          if(f['mimeType'] == "application/vnd.google-apps.spreadsheet"):
            #print("Found metadata; retrieving folder structure.  If you want to check all files for a release, set onlyMeta to False.")
            downloadFile(service, spaces, f['name'],
                       f['id'], f['mimeType'], d_folder)
            #return "Complete"


def download_from_gDrive(boundary_version, checkMeta, checkBoundary):
    basedir = os.path.dirname("")
    global num_files
    num_files = 0
    global num_skiped
    num_skiped = 0
    global from_dir
    path = boundary_version + "/"
    from_dir = path
    global to_dir
    to_dir = to_dir + "/"
    #print("Downloading folder and files from: {} -> to: {}".format(from_dir, to_dir))
    #print("Connecting with Google Drive")

    try:
        credentials = get_credentials()
        service = discovery.build('drive', 'v3', credentials=credentials)
    except Exception as e:
        print("Error connecting to Google Drive")
        print(e)
    else:

        #print("Connected. Now let´s read the files")
        if listing:
            #print("just listing folder and files from source")
            prepDest(to_dir, "")
            folderId = getFolderId(service, from_dir)
            if not folderId is None:
                getFolderFiles(service, folderId, from_dir, to_dir, 0, checkMeta, checkBoundary)
            else:
              1 == 1
                #print("Aborting. Source folder {} not found".format(from_dir))

        elif prepDest(to_dir, ""):
            #print("Downloading files")
            folderId = getFolderId(service, from_dir)
            if not folderId is None:
                getFolderFiles(service, folderId, from_dir, to_dir, 0, checkMeta, checkBoundary)
                #print("{} total files downloaded.\n".format(num_files))
                if num_skiped > 0:
                    print("{} total skiped files, not downloaded.".format(num_skiped))
            else:
                print("Aborting. Source folder {} not found".format(from_dir))
                1 == 1
        else:
            #print("Destination folder : {} - already exists".format(to_dir))
            1 == 1
    if(num_files > 0):
      return "PASSED"

