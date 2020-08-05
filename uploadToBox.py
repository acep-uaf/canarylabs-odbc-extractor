from boxsdk import Client, OAuth2
from boxsdk.network.default_network import DefaultNetwork
from pprint import pformat
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

def uploadZippedToBox(zipFolder):
    '''uploads new zip folders to box. Will not upload a zip folder if it already exists on Box even if the contents have changed'''
    #files to upload
    allFiles = os.listdir(zipFolder)
    zipFiles = [f for f in allFiles if f[-3:] == 'zip']
    # Define client ID, client secret, and developer token.
    path = "C:\\Users\\tcmorgan2\\CanaryLabsDataManagement"
    # Read app info from text file
    config = ConfigObject(os.path.join(path, 'Boxapp.cfg'))
    CLIENT_ID = config['client_id']
    
    ACCESS_TOKEN = config['access_token']

    # Create OAuth2 object.
    auth = OAuth2(client_id=CLIENT_ID, client_secret='', access_token=ACCESS_TOKEN)
    # Create the authenticated client
    client = Client(auth)

    # make sure we connected
    try:
        my = client.user(user_id='me').get()
        print(my.name) #developer name tied to the token
    except :
        sys.exit("ERROR: Invalid access token; try re-generating an "
                 "access token from the app console on the web.")

    tfolder = client.folder('111885558694') #2020 scada data folder

    items = tfolder.get_items()
    for item in items:
        if item.name in zipFiles:
            zipFiles.remove(item.name)
    uploadedFiles = []
    badUploads = []
    here = os.getcwd()
    for zipped in zipFiles:
         os.chdir(zipFolder)
         try:
             box_file = tfolder.upload(zipped)
             uploadedFiles.append(zipped)
         except Exception as e:
             print(e)
             badUploads.append(zipped)
             pass
         finally:
             os.chdir(here)
    return uploadedFiles, badUploads

