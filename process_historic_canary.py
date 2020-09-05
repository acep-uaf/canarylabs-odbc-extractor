import pyodbc
import csv
import os
import datetime
from zipfile import ZipFile
import dropboxupdate
import uploadToBox
from Canary2Timescale import Canary2Timescale, writeChannels

'''script to read data out of canary labs historian and create a crosstab
output as csv containing a single day of data for each parameters'''
#CONSTANTS
DATAFOLDER = 'C:\\HistorianData\\2020'
DATEFORMAT = "%Y-%m-%d %H:%M:%S.%f"
BYCHANNEL = os.path.join(DATAFOLDER,'ByChannel')
KEYCHANNELS = os.path.join(DATAFOLDER,'KeyChannels')

def writeRecords(records,filepath):
    '''write a csv file of raw table data extracted from the Historian data table'''
    header = []
    if len(records)>0:
        if not os.path.exists(filepath):
            header = ['tag_name','description','time_stamp','value','quality']
        with open(filepath, mode='a',newline='') as Mycsv:
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
    def fillZip(f,zipObj):
        s = zipObj.write(f)
        os.remove(f)
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
def zipByDays(myDirectory):
    files = os.listdir(myDirectory)
    os.chdir(myDirectory)
    def fillZip(f,zipObj):
        s = zipObj.write(f)
        os.remove(f)
    for y in years:
        for m in months:
            for d in days:
                matchString = str(y) + "-" + '{:02d}'.format(m) + "-" + '{:02d}'.format(d)
                print(matchString)
                zipFiles = [f for f in files if matchString in f and f[-3:] == 'csv']
                if len(zipFiles) > 0:
                    alreadyZipped = os.listdir(myDirectory)
                    if ((matchString + ".zip") not in alreadyZipped):
                        with ZipFile(matchString + ".zip", 'w') as zipObj:
                            [fillZip(f,zipObj) for f in zipFiles]
    return
#The historian requires datetime filters
#for instance 'select * from data' returns only records from the most recent hour 
#so we run through day by day
    
#The connnection

# def writeLast(d):
#     lastPath = "lastRecord.txt"
#     try:
#         with open(lastPath, 'w+') as lastfile:
#             lastfile.write(datetime.datetime.strftime(d,"%Y-%m-%d %H:%M:%S.%f")) # The variable to store the last datetimestamp
#
#     except Exception as e:
#         print(e)

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
    return [getDateTime(d) for d in listOfFiles]
#dstart = datetime.datetime.strptime('2019-01-01 00:00:00.100000',"%Y-%m-%d %H:%M:%S.%f")
#dend = datetime.datetime.strptime('2020-01-01 00:00:00.100000',"%Y-%m-%d %H:%M:%S.%f")

def readSpecialChannelNames():
    return []
def main():

    # newFiles = dropboxupdate.getFiles() #test case removed Cordova 20200415 00 so one file should be processed
    # Canary2Timescale(min(fileDateTime(newFiles)))
    # writeChannels(min(fileDateTime(newFiles)),datetime.datetime.now(),BYCHANNEL,None)
    # specialChannels = readSpecialChannelNames()
    # writeChannels(min(fileDateTime(newFiles)),datetime.datetime.now(),KEYCHANNELS,specialChannels)
    # zipByDays(BYCHANNEL)
    # zipByDays(KEYCHANNELS)
    # success, failed = uploadToBox.uploadZippedToBox(BYCHANNEL)
    # for z in success:
    #     os.rmdir(z)
    # return {'downloaded':len(newFiles),'uploaded':len(success),'failedUploads':len(failed)}
    Canary2Timescale(datetime.datetime.strptime('2020-01-06 18',"%Y-%m-%d %H"))
if __name__ == '__main__':
    main()
