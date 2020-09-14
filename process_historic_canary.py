import subprocess

import pyodbc
import csv
import os
import datetime
from zipfile import ZipFile
import dropboxupdate
import uploadToBox
from Canary2Timescale import Canary2Timescale, writeData, refreshViews

'''script to read data out of canary labs historian and create a crosstab
output as csv containing a single day of data for each parameters'''
#CONSTANTS
DATAFOLDER = 'C:\\ProcessedCordovaData'
DATEFORMAT = "%Y-%m-%d %H:%M:%S.%f"
BYCHANNEL = os.path.join(DATAFOLDER,'ByChannel')
KEYCHANNELS = os.path.join(DATAFOLDER,'KeyChannels')

def writeRecords(records,filepath):
    '''write a csv file of table data extracted from the Historian data table'''
    header = []
    if len(records)>0:
        if not os.path.exists(filepath):
            header = ['tag_name','description','time_stamp','value','quality']
        if not os.path.exists(os.path.dirname(filepath)):
            os.mkdir(os.path.dirname(filepath))
        with open(filepath, mode='a+',newline='') as Mycsv:
            csv_writer = csv.writer(Mycsv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            if len(header) > 0:
                csv_writer.writerow(header)

            [csv_writer.writerow(row) for row in records]

    return

def checkCreatePath(paths):
    '''checks for the presense of individual folders in a list of file paths and creates them
    if they don't already exist'''
    if len(paths) <=0:
        return
    else:
        if not os.path.exists(paths[0]):
            os.mkdir(paths[0])
        paths.pop(0)
        return checkCreatePath(paths)


def zipByMonth(myDirectory):
    files = os.listdir(myDirectory)
    os.chdir(myDirectory)

    for y in years:
        for m in months:
            matchString = str(y)+ "-" + '{:02d}'.format(m)
            zipFiles = [f for f in files if matchString in f]
            if len(zipFiles) > 0:
                with ZipFile(matchString + ".zip", 'w') as zipObj:
                    [fillZip(f,zipObj) for f in zipFiles]

years = [2018,2019,2020]
months = list(range(1,13,1))
days = list(range(1,32,1))


def fillZip(f, zipObj):
    s = zipObj.write(f, os.path.basename(f))
    os.remove(f)
    return
def zipByDays(myDirectory,prefix):
    files = os.listdir(myDirectory)
    os.chdir(myDirectory)

    for y in years:
        for m in months:
            for d in days:
                matchString = str(y) + "-" + '{:02d}'.format(m) + "-" + '{:02d}'.format(d)
                print(matchString)
                zipFiles = [f for f in files if matchString in f and f[-3:] == 'csv']
                if len(zipFiles) > 0:
                    alreadyZipped = os.listdir(myDirectory)
                    if ((matchString + ".zip") not in alreadyZipped):
                        with ZipFile(prefix + matchString + ".zip", 'w') as zipObj:
                            [fillZip(f,zipObj) for f in zipFiles]
    return

def makeSingleFile(directory, tag):
    files = os.listdir(directory)
    monthChannelList=[]
    os.chdir(directory)
    for f in files:
        if tag.replace(".","_") in f:

            with open(f,newline='') as infile:
                csv_reader = csv.reader(infile,delimiter=',',quotechar='"')
                mon =f[-9:-7]
                with open(os.path.join("..",tag + mon + ".csv"), mode='a',newline='') as channelFile:
                    csv_writer = csv.writer(channelFile,delimiter=',',quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    if (tag + mon) not in monthChannelList:
                        monthChannelList.append(tag + mon)
                        csv_writer.writerow(['tag_name','description','time_stamp','value','quality']) #write a header
                    [csv_writer.writerow(row) for row in csv_reader if row[0] != 'tag_name']
def getDateTime(filename):
    '''
    Returns a date object extracted from the file name for hdb files. Assumes a naming convention of Cordova yyyymmdd hh.hdb
    :param filename:
    :return:
    '''
    strday = filename.split(" ")[1]
    strhour = filename.split(" ")[2][0:2]
    return datetime.datetime(int(strday[0:4]),int(strday[4:6]), int(strday[6:8]), int(strhour))
def fileDateTime(listOfFiles):
    '''return the minimum start time contained within a file
    file names contain the min timestamp included in the file, and a 24 hour period after that value'''
    return [getDateTime(d) for d in listOfFiles]

def readSpecialChannelNames():
    PrimaryChannels = []
    with open(os.path.join(*[os.path.dirname(os.path.abspath(__file__)),"instance","SCADA channels of interestTDK.csv"]), newline = '') as channels:
        lines = csv.reader(channels, delimiter=',')
        for row in lines:
            PrimaryChannels.append(row[5]) #row is a tuple, position 5 contains tag name
    return PrimaryChannels
def restartHistorian():
    os.chdir("bash")
    # service will only restart if run as administator
    subprocess.call("restart_historian.bat",shell=True)
    os.chdir('..')
    return
def zipAndLoad(zipPath, folderDate,result):
    '''
    :param zipPath is the path to the folder contaiint data to zip
    '''
    if KEYCHANNELS in zipPath:
        prefix = "PrimaryChannel"
    else:
        prefix = ""
    zipFiles = [os.path.join(zipPath,f) for f in os.listdir(zipPath) if (f[-3:] =='csv') & (folderDate in f)]
    if len(zipFiles) > 0:
        alreadyZipped = [ f for f in os.listdir(zipPath) if f[-3:]=='zip']
        if ((folderDate + ".zip") not in alreadyZipped):
            with ZipFile(os.path.join(zipPath,prefix + folderDate + ".zip"), 'w') as zipObj:
                [fillZip(f, zipObj) for f in zipFiles]
    uploaded = uploadToBox.uploadZippedToBox(os.path.join(zipPath, prefix + folderDate + ".zip"))
    if uploaded:
        os.remove(os.path.join(zipPath, prefix + folderDate + ".zip"))
    result.put(prefix + folderDate + ".zip", uploaded)
    return

def main():
    newFiles = dropboxupdate.getFiles() # get any new hdb files that have been posted
    # restart historian service or new files won't be recognized
    if len(newFiles)>0:
        restartHistorian()
    newFileDates = [fileDateTime(newFiles)][0]
    newFileDates.sort() #the last file is often not a complete 24 hours
    Canary2Timescale(newFileDates)

    success, failed = writeData(None,newFileDates, BYCHANNEL)
    specialChannels = readSpecialChannelNames()

    success2, failed2 = writeData(specialChannels,newFileDates,KEYCHANNELS)
    # TODO finish mviews
    # refreshViews(newFileDates)
    return {'downloaded':len(newFiles),'uploaded':len(success),'failedUploads':len(failed)}
if __name__ == '__main__':
    main()
