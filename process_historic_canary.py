import pyodbc
import csv
import os
import datetime
from zipfile import ZipFile
import dropboxupdate

'''script to read data out of canary labs historian and create a crosstab
output as csv containing a single day of data for all parameters'''
#CONSTANTS
DATAFOLDER = 'D:\\RADIANCE\\Canary_Labs_Data_Management\\canarylabs-odbc-extractor\\Cordova_SCADA_Data\\HistorianData2019'
DATEFORMAT = "%Y-%m-%d %H:%M:%S.%f"
BYCHANNEL = os.path.join(DATAFOLDER,'ByChannel')

       
def writeRecords(records,filepath):
    '''write a csv file of raw table data extracted from the Historian data table'''
    header = []

    if not os.path.exists(filepath):
        header = ['tag_name','description','time_stamp','value','quality']
    with open(filepath, mode='a',newline='') as Mycsv:
        csv_writer = csv.writer(Mycsv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        if len(header) > 0:
            csv_writer.writerow(header)
    
        [csv_writer.writerow(row) for row in records]
        

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


years = [2018,2019]
months = months = list(range(1,13,1))
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
                
#The historian requires datetime filters
#for instance 'select * from data' returns only records from the most recent hour 
#so we run through day by day
dstart = datetime.datetime.strptime('2019-01-01 00:00:00.100000',"%Y-%m-%d %H:%M:%S.%f")
dend = datetime.datetime.strptime('2020-01-01 00:00:00.100000',"%Y-%m-%d %H:%M:%S.%f")

    
#The connnection
def processData(d1,dend,tries = 0):
    logFile = open(os.path.join(*[BYCHANNEL,datetime.datetime.now().strftime("%Y-%m-%d %H%M%S")+'log.txt']),"w+")
    pyodbc.pooling = False
    try:
        cnxn = pyodbc.connect('DSN=CanaryODBCClient;')
        cursor = cnxn.cursor()
        tags = cursor.execute("SELECT * FROM tags WHERE tag_name like '%.Cordova.%'").fetchall()
        for t in tags:
            print(t[0])
            d1 = dstart
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
                        writeRecords(records,filepath)             

                    
                except pyodbc.Error as er:
                    logFile.write("pyodbc error for channel {0} on {1} \n".format(t[0],d2))
                    logFile.write(str(er) +"/n")
                   
                    cursor.cancel()
                    if 'retrieving data for column' in er.args[1]:
                        logFile.write("Moving to next date value\n")
                        
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
                                                  
    except:
        raise
    finally:      
        logFile.close()
        cnxn.close()

def main():
    #dropboxupdate.main()
    processData(dstart,dend)
    zipByMonth(BYCHANNEL)

if __name__ == '__main__':
    main()
