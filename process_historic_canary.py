import pyodbc
import csv
import os
import datetime 

'''script to read data out of canary labs historian and create a crosstab
output as csv containing a single day of data for all parameters'''
#CONSTANTS
datafolder = 'HistoricData2018'
dateformat = "%Y-%m-%d %H:%M:%S.%f"

def putInDictionary(records):
    '''Put a list of tuples into a dictionary. Each time_stamp gets its own row and all tags get put in their corresponding time_stamped row.
    records is a list of tuples organized as ('tag_name','description','time_stamp','value','quality')
    '''
    fileDict={}
    tags = set()
    for row in records:
        tags.add(row[1])
        time_stamp = row[2].strftime(dateformat)
        tag = row[1]    
        rowdata = {'time_stamp':time_stamp,tag + '_value':row[3], tag +'_quality': row[4]}
        if time_stamp in fileDict.keys():
            fileDict[time_stamp].update(rowdata) #add the new tag values to the row
        else:
            fileDict[time_stamp] = rowdata
    headers = [t + '_quality' for t in tags] + [t + '_value' for t in tags]
    headers.sort()
    fileDict['headers'] = ['time_stamp'] + headers
    return fileDict
    
def writeDictionary(dayDict,fileTag):
    ''' write a dictionary of data rows to a csv file
    filetag is a string prefix for the csv file name
    dayDict is a dictionary of data rows with a key named 'header' that contains column names
    '''
    headers = dayDict['headers']
    newcsvName = fileTag + 'allChannels.csv'
    with open(os.path.join(datafolder,newcsvName), mode='w',newline='') as Mycsv:
        csv_writer = csv.DictWriter(Mycsv,fieldnames=headers)
        csv_writer.writeheader()
        for r in dayDict.keys(): #r will be the rows timestamp
            if r != 'headers':
                csv_writer.writerow(dayDict[r])
           

#The historian requires filters
#for instance 'select * from data' results in fewer records that 'select * from data where time_stamp < 2019-01-01'
#so we run through day by day
d1 = datetime.datetime.strptime('2018-01-01 00:00:00.1',"%Y-%m-%d %H:%M:%S.%f")
dend = datetime.datetime.strptime('2019-05-23 00:00:01.0',"%Y-%m-%d %H:%M:%S.%f")
d2 = d1 + datetime.timedelta(hours=24)

#The connnection
cnxn = pyodbc.connect('DSN=CanaryODBCClient;')
cursor = cnxn.cursor()
#extract and write data for each day 
while d2 < dend:
    try:
        #data as list of tuples
        records = cursor.execute("SELECT * from data WHERE time_stamp < '" + d2.strftime(dateformat) + "' and tag_name like 'ACEP-WS-DWINBOX.Cordova%'").fetchall()
        #tuples are organized as ('tag_name','description','time_stamp','value','quality')
        dayDict = putInDictionary(records)
        if dayDict != None:
            writeDictionary(dayDict,(d2.strftime(dateformat)).split(' ')[0])
    except Exception as e:
        print(d2)
        print(e)
    finally:
        d2 = d2 + datetime.timedelta(hours=24)
#close database connection
cnxn.close()



    
    
