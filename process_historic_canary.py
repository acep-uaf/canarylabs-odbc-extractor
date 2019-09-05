import pyodbc
import csv
import os
import datetime 

'''script to read data out of canary labs historian and create a crosstab
output as csv containing a single day of data for all parameters'''
#CONSTANTS
DATAFOLDER = 'PowerOutage20190814'
DATEFORMAT = "%Y-%m-%d %H:%M:%S.%f"
RAW = os.path.join(DATAFOLDER,'rawData')
COMBINED = os.path.join(DATAFOLDER,'CombinedChannels')
BYCHANNEL = os.path.join(DATAFOLDER,'ByChannel')

def putInDictionary(records):
    '''Put a list of tuples into a dictionary. Each time_stamp gets its own row and all tags get put in their corresponding time_stamped row.
    records is a list of tuples organized as ('tag_name','description','time_stamp','value','quality')
    '''
    fileDict={}
    channelDict={} #a dictionary of dictionaries. Each key is a channel name
    tags = set()
    for row in records:
        tags.add(row[1])
        time_stamp = row[2].strftime(DATEFORMAT)
        tag = row[1]  #tag is the name of the channel
        rowdata = {'time_stamp':time_stamp,tag + '_value':row[3], tag +'_quality': row[4]}
        channelData = {'time_stamp':time_stamp,'value':row[3], 'quality': row[4]}
        if tag in channelDict.keys():
            channelDict[tag][time_stamp] = channelData
           
        else:
            tagDict = {}
            tagDict['headers']=['time_stamp','value','quality']
            tagDict[time_stamp] = channelData
            channelDict[tag] = tagDict
                       
        if time_stamp in fileDict.keys():
            fileDict[time_stamp].update(rowdata) #add the new tag values to the row
        else:
            fileDict[time_stamp] = rowdata
    headers = [t + '_quality' for t in tags] + [t + '_value' for t in tags]
    headers.sort()
    fileDict['headers'] = ['time_stamp'] + headers
    return fileDict,channelDict


def writeDictionary(dayDict,filePath):
    ''' write a dictionary of data rows to a csv file
    filePath is the path to the csv file including file name and extension
    dayDict is a dictionary of data rows with a key named 'header' that contains column names
    '''
    headers = dayDict['headers']
    
    with open(filePath, mode='w',newline='') as Mycsv:
        csv_writer = csv.DictWriter(Mycsv,fieldnames=headers)
        csv_writer.writeheader()
        for r in dayDict.keys(): #r will be the rows timestamp
            if r != 'headers':
                csv_writer.writerow(dayDict[r])
          
def writeRecords(records,filepath):
    '''write a csv file of raw table data extracted from the Historian data table'''
    
    header = ['tag_name','description','time_stamp','value','quality']
    with open(filepath, mode='w',newline='') as Mycsv:
        csv_writer = csv.writer(Mycsv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(header)
        for row in records:
            csv_writer.writerow(row)

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

def writeChannelDictionary(channelDict, filePrefix):
  
    for d in channelDict.keys():
        if d != 'headers':
            writeDictionary(channelDict[d],filePrefix + d + '.csv') #each key is a channel
        

                
#The historian requires filters
#for instance 'select * from data' results in fewer records that 'select * from data where time_stamp < 2019-01-01'
#so we run through day by day
d1 = datetime.datetime.strptime('2019-08-13 00:00:00.100000',"%Y-%m-%d %H:%M:%S.%f")
dend = datetime.datetime.strptime('2019-08-17 00:00:00.100000',"%Y-%m-%d %H:%M:%S.%f")
d2 = d1 + datetime.timedelta(hours=24)

#The connnection
cnxn = pyodbc.connect('DSN=CanaryODBCClient;')
cursor = cnxn.cursor()

#make sure data folders exist, or; create them
checkCreatePath([RAW,BYCHANNEL,COMBINED])

#extract and write data for each day 
while d2 < dend:
    try:
        #data as list of tuples
        records = cursor.execute("SELECT * from data WHERE time_stamp < '" + d2.strftime(DATEFORMAT) + "' and tag_name like 'ACEP-WS-DWINBOX.Cordova%'").fetchall()
        #records = cursor.execute("SELECT * from data WHERE time_stamp < '2019-07-30 00:00:00.100000'").fetchall()
        #tuples are organized as ('tag_name','description','time_stamp','value','quality')
        newcsvName = ((d2 - datetime.timedelta(hours=24)).strftime(DATEFORMAT)).split(' ')[0] + 'rawData.csv'
        filepath = os.path.join(*[RAW,newcsvName])
        
        #write the raw data
        writeRecords(records,filepath)
        #convert data to dictionaries for re-formatting csv's
        dayDict,channelDict = putInDictionary(records)
        
        #write the combined csv
        if dayDict != None:
            newcsvName = ((d2 - datetime.timedelta(hours=24)).strftime(DATEFORMAT)).split(' ')[0] + 'allChannels.csv'
            filepath = os.path.join(*[COMBINED,newcsvName])
            writeDictionary(dayDict,filepath)
            
        #write the individual channels to csv
        if channelDict != None:
            newcsvName = ((d2 - datetime.timedelta(hours=24)).strftime(DATEFORMAT)).split(' ')[0]
            filepath = os.path.join(*[BYCHANNEL,newcsvName]) #this is only the file prefix
            writeChannelDictionary(channelDict,filepath)
    except Exception as e:
        print(d2)
        print(e)
    finally:
        d2 = d2 + datetime.timedelta(hours=24)
#close database connection
cnxn.close()



    
    
