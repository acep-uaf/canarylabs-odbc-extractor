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
DATAFOLDER = 'D:\\RADIANCE\\Canary_Labs_Data_Management\\canarylabs-odbc-extractor\\Cordova_SCADA_Data\\HistorianData2020'
DATEFORMAT = "%Y-%m-%d %H:%M:%S.%f"
BYCHANNEL = os.path.join(DATAFOLDER,'ByChannel')
KEYCHANNELS = os.path.join(DATAFOLDER,'KeyChannels')
LASTRECORDFile = 'D:\\RADIANCE\\Canary_Labs_Data_Management\\canarylabs-odbc-extractor\\lastRecord.txt'

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
def processData(dstart,dend,tries = 0):
    '''processes Historian data into csv files. Logs dates when data cannot be processed and throws processing error if
    can't connect or fails to process data. Returns the last date processed'''
    logFile = open(os.path.join(*[BYCHANNEL,datetime.datetime.now().strftime("%Y-%m-%d %H%M%S")+'log.txt']),"w+")
    pyodbc.pooling = False
    lastDate = dstart
    d1 = dstart
    try:
        cnxn = pyodbc.connect('DSN=CanaryODBCClient;')
        cursor = cnxn.cursor()
        tags = cursor.execute("SELECT * FROM tags WHERE tag_name like '%.Cordova.%'").fetchall()
        # tags = wantedTags
        for t in tags:
            d1 = dstart
            print(t)
            d2 = d1 + datetime.timedelta(days=1)
             #extract and write data for each day
            while d2 < dend:
                try:          
                    #data as list of tuples
                    records= cursor.execute("SELECT tag_name, description, time_stamp, value, quality from data WHERE time_stamp > '" + d1.strftime(DATEFORMAT) + "'AND time_stamp < '" + d2.strftime(DATEFORMAT) + "' and tag_name = '" + t[0] +"'").fetchall()
                    #tuples are organized as ('tag_name','description','time_stamp','value','quality')
                    newcsvName = t[0].replace(".","_") + (d1.strftime(DATEFORMAT)).split(' ')[0] +".csv"
                    filepath = os.path.join(*[BYCHANNEL,newcsvName]) #this is only the file prefix

                    if len(records) > 0:
                        if d2 > lastDate:
                            lastDate = d2

                        writeRecords(records,filepath)

                except pyodbc.Error as er:
                    logFile.write("pyodbc error for channel {0} on {1} \n".format(t[0],d2))
                    logFile.write(str(er) +"\n")
                   
                    cursor.cancel()
                    if 'retrieving data for column' in er.args[1]:
                        logFile.write(str(datetime.datetime.now()) + "Moving to next date value\n")
                        pass
                    else:
                         tries = tries +1
                         
                         if tries < 10:
                            pass
                         else:
                            cnxn.close()
                            logFile.write("connections closed\n")
                            try:
                                cnxn = pyodbc.connect('DSN=CanaryODBCClient;')
                                cursor = cnxn.cursor()
                            except: 
                                raise
                except Exception as e:
                    logFile.write("Exception for channel {0} on {1}".format(t[0],d2))
                    logFile.write(str(e))  
                d1 = d1 + datetime.timedelta(days=1)#bump up d1
                d2 = d1 + datetime.timedelta(days=1)#bump up d2

    except Exception as e:
        raise ProcessingStoppedError(d1,e)
    finally:      
        logFile.close()
        cnxn.close()
    return lastDate

class ProcessingStoppedError(BaseException):
    def __init__(self,dt,msg):
        self.datetime = dt
        self.message = msg

def getLastRecord():

    try:
        lastfile = open(LASTRECORDFile, 'r')
        last = lastfile.readline()  # The variable to store the last datetimestamp
        lastfile.close()  # close the file
        return datetime.datetime.strptime(last,"%Y-%m-%d %H:%M:%S.%f")
    except Exception as e:
        print(e)
        last = '2019-06-30 00:00:00'

    print('Retrieving data back to',last)
    return last

def writeLast(d):
    lastPath = "lastRecord.txt"
    try:
        with open(lastPath, 'w+') as lastfile:
            lastfile.write(datetime.datetime.strftime(d,"%Y-%m-%d %H:%M:%S.%f")) # The variable to store the last datetimestamp

    except Exception as e:
        print(e)

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

    newFiles = dropboxupdate.getFiles() #test case removed Cordova 20200415 00 so one file should be processed
    Canary2Timescale(min(fileDateTime(newFiles)))
    writeChannels(min(fileDateTime(newFiles)),datetime.datetime.now(),BYCHANNEL,None)
    specialChannels = readSpecialChannelNames()
    writeChannels(min(fileDateTime(newFiles)),datetime.datetime.now(),KEYCHANNELS,specialChannels)
    zipByDays(BYCHANNEL)
    zipByDays(KEYCHANNELS)
    success, failed = uploadToBox.uploadZippedToBox(BYCHANNEL)
    for z in success:
        os.rmdir(z)
    return {'downloaded':len(newFiles),'uploaded':len(success),'failedUploads':len(failed)}

if __name__ == '__main__':
    main()
