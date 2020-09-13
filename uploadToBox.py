from boxsdk import Client, OAuth2

import os
import sys

def ConfigObject(config_path):
    "read a configuration file to retrieve access token"
    configDict = {}
    with open(config_path,'r') as config:
        for line in config.readlines():
            try:
                configDict[line.split("=")[0]] = line.split("=")[1].rstrip()
            except:
                pass
    return configDict
def uploadZippedToBox(zippedFolder, boxfolder = None):
    if boxfolder is None:
        boxfolder = accessUploadFolder()
    try:
        items = boxfolder.get_items()
        for item in items:
            if item.name == os.path.basename(zippedFolder):
                return False
        boxfolder.upload(zippedFolder)
        uploaded = True
    except Exception as e:
        print(e)
        uploaded = False
        pass
    finally:
        return uploaded

def accessUploadFolder(year=2020):
    # Define client ID, client secret, and developer token.
    path = "instance"
    # Read app info from text file
    config = ConfigObject(os.path.join(path, 'Boxapp.cfg'))
    CLIENT_ID = config['client_id']
    CLIENT_FOLDER = config['client_folder' + str(year)]
    ACCESS_TOKEN = config['access_token']

    # Create OAuth2 object.
    auth = OAuth2(client_id=CLIENT_ID, client_secret='', access_token=ACCESS_TOKEN)
    # Create the authenticated client
    client = Client(auth)

    # make sure we connected
    try:
        my = client.user(user_id='me').get()
        print(my.name)  # developer name tied to the token
    except:
        sys.exit("ERROR: Invalid access token; try re-generating an "
                 "access token from the app console on the web.")

    tfolder = client.folder(CLIENT_FOLDER)  # 2020 scada data folder
    return tfolder
def listZipFiles(directory_folder):
    '''
    Lists teh zip folders in teh directory folder, including subdirectortories
    '''
    zipFiles = []
    for root, dirs, files in os.walk(directory_folder):
        for name in files:
            if name[-3:] == 'zip':
                zipFiles.append(os.path.join(root, name))
    return zipFiles
def uploadAllZippedToBox(zipFolder):
    '''uploads new zip folders to box. Will not upload a zip folder if it already exists on Box even if the contents have changed'''
    #files to upload
    zipFiles = listZipFiles(zipFolder)
    tfolder = accessUploadFolder()
    items = tfolder.get_items()
    for item in items:
        if item.name in zipFiles:
            zipFiles.remove(item.name)
    uploadedFiles = []
    badUploads = []

    for zipped in zipFiles:
         try:
             uploadZippedToBox(zipped, tfolder)
             uploadedFiles.append((zipped,True))
         except Exception as e:
             print(e)
             badUploads.append((zipped,False))
             pass

    return uploadedFiles, badUploads

