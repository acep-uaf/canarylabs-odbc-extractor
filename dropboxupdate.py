"""Syncrhonize D:HistorianData folder with dropbox.
"""

import asyncio
import urllib
import dropbox
import os
import sys

from dropbox.exceptions import AuthError

DROPBOX_URL = "https://www.dropbox.com/scl/fo/pjgnsgq98aw1xoan22imd/AAAXO5qNZT9NZN4h7ILsOSp7a/Histlog?dl=0"
DATAFOLDER = "D:\\Historian Data"

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


def getFileList(basefolder):
    "list the files we already have - won't overwrite these files"
    files = []
    try:
        
        for root, dirnames, filenames in os.walk(basefolder):
            for filename in filenames:
                files.append(filename)   
    except Exception as e:
        print(e)
    finally:
        return files
#
def processEntries(dbx,url,currentFiles, entries):
    """

    :param dbx: dropbox connection
    :param url: url of the folder containing new files
    :param currentFiles: files already stored locally
    :param entries: filemetadata returned from list_files on linked folder
    :return: None, writes file to data folder
    """
    print("processing:",len(entries))
    originaldirectory = os.getcwd()
    os.chdir(DATAFOLDER)
    try:
        for fm in entries:
            if (fm.name not in currentFiles): # can add  'and (fm.name.lower()[0:12] == 'cordova 2019')' if targetting a specific subfolder
                dlink, response = dbx.sharing_get_shared_link_file(url, "/" + fm.name)

                with open(fm.name, "wb") as f:
                    print("adding: ",fm.name)
                    f.write(response.content)
    except Exception as e:
        print(e)
        pass
    finally:
        os.chdir(originaldirectory)


def download(dbx):
    """
    lists all files contained in linked folder and writes any new files to the local data folder
    :param dbx: dropbox connection
    :return: None, files written to data folder
    """
    currentFiles = getFileList(DATAFOLDER)
    shared_link = dropbox.files.SharedLink(url=DROPBOX_URL)
    files =  dbx.files_list_folder(path="", shared_link=shared_link)
   
    processEntries(dbx, DROPBOX_URL, currentFiles, files.entries)
    while files.has_more:
        files = dbx.files_list_folder_continue(files.cursor)
        processEntries(dbx, DROPBOX_URL, currentFiles, files.entries)


def main():
    """download the contents of a given linked dropbox folder"""
    #path = "C:\\Users\\tcmorgan2\\CanaryLabsDataManagement" #ALTER as need for accessing user config file
    path = os.path.dirname(__file__)
    config = ConfigObject(os.path.join(path,'config.ini')) #read config file with access token
    dbx = dropbox.Dropbox(config['access_token'])
    #make sure we connected
    try:
        dbx.users_get_current_account()
    except AuthError:
        sys.exit("ERROR: Invalid access token; try re-generating an "
                 "access token from the app console on the web.")
    #download the contents
    print("updating data contents")
    download(dbx)
    
          

if __name__ == '__main__':
    main()
