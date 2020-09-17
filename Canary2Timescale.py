# Projet: canarylabsdataextractor
# Created by: # Created on: 9/3/2020
# Purpose :  Canary2Timescale
import os
import psycopg2
import datetime
import pyodbc
import pytz
#CONSTANTS
import process_historic_canary
import multiprocessing as mp
from makeMaterial import refreshMaterial, makeMaterial

DATEFORMAT = "%Y-%m-%d %H:%M:%S.%f"

class HistorianDay:
    def __init__(self,d):
        self.begin = datetime.datetime.combine(d, datetime.datetime.min.time())
        self.end = datetime.datetime.combine(d, datetime.datetime.max.time())
        self.strday = '{:04d}'.format(d.year) + "-" + '{:02d}'.format(d.month) + "-" + '{:02d}'.format(d.day)

def readPGconfig():
    "read a configuration file to retrieve access token"
    configDict = {}
    with open(os.path.join(*[os.path.dirname(os.path.abspath(__file__)), 'instance','pgconfig.cfg']), 'r') as config:
        for line in config.readlines():
            try:
                configDict[line.split("=")[0]] = line.split("=")[1].rstrip()
            except:
                pass
    return configDict
def connect2Canary():
    return pyodbc.connect('DSN=CanaryODBCClient;')
def connect2Pg():
    constring = "dbname = cordova_scada_data user=" + str(readPGconfig()['user']) +  " password=" + str(readPGconfig()['pwd'])
    return psycopg2.connect(constring)
def readCanary(cnxn,start,stop,tag):

    cursor = cnxn.cursor()
    try:
        if tag is None:
            records = cursor.execute(
                "SELECT tag_name,time_stamp, value, quality from data WHERE time_stamp > '" + start.strftime(
                    DATEFORMAT) + "'AND time_stamp < '" + stop.strftime(DATEFORMAT) + "'").fetchall()
        else:
            records = cursor.execute(
                "SELECT tag_name, description, time_stamp, value, quality from data WHERE time_stamp > '" + start.strftime(
                DATEFORMAT) + "'AND time_stamp < '" + stop.strftime(DATEFORMAT) + "' and tag_name = '" + tag[0] + "'").fetchall()

    except Exception as e:
       records =[]
       with open(datetime.datetime.today().strftime("%Y-%m-%d") + 'log.txt', "a+") as logFile:
            logFile.write("pyodbc error on {0} \n".format(stop.strftime(DATEFORMAT)))
            logFile.write(str(e) + "\n")
    finally:
        return records

def daterange(date1, date2):
    for n in range(int((date2 - date1).days) + 1):
        yield date1 + datetime.timedelta(n)

def getLastInsert(cnx):
    cursor = cnx.cursor()
    cursor.execute("SET session time zone 'America/Anchorage'")
    cursor.execute("SELECT max(datetime) from data")
    last = cursor.fetchone()
    cursor.close()
    if last != (None,):
        return last
    return None

def write2Pg(cnx,records):
    '''

    :param cnx: connection to postgresql database
    :param records: list of tuples with the format datetime, channel, value, quality
    :return: None
    '''
    cursor = cnx.cursor()
    cursor.execute("SET session time zone 'America/Anchorage'")
    cursor.executemany("INSERT INTO data(channel,datetime,value,quality) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING",records)
    cnx.commit()
    cursor.close()
    return

def getAllTags(start,stop):
    ''' Return a list of strings of all the channel names used within a given time period between start and stop
    :param: start datetime object
    :param: stop datetime object
    '''
    cnx = connect2Pg()
    cursor = cnx.cursor()
    tags = []
    start = start.strftime(DATEFORMAT)
    stop = stop.strftime(DATEFORMAT)
    try:
        cursor.execute(
                "SELECT DISTINCT channel FROM data WHERE datetime BETWEEN %s AND %s",(start,stop))
        tagtuples = cursor.fetchall()
        tags = [".".join(t[0].split(".")[1:]) for t in tagtuples]

    finally:
        cursor.close()
        cnx.close()
    return tags

def writeData(tags,filedays,datapath):
    '''Write records from the data table for dates provided as csv files to a specified data folder.
    CSV files are grouped by day into zip folders and uploaded to Box.
    :param tags: list of channel names to export. Each channel will have its own csv file
    :param filedays: list of dates to export. Each day channel combination gets its own csv file
    :param datapath: The folder that csv files will be written to.'''
    cnx = connect2Pg()
    success = []
    failed = []
    if len(filedays) >0:
        result = mp.Manager().Queue()
        pool = mp.Pool(mp.cpu_count())
        try:
            if tags is None:
                tags = getAllTags(HistorianDay(min(filedays)).begin, HistorianDay(max(filedays)).end)

            for myday in filedays:
                d = HistorianDay(myday)
                zipPath = os.path.join(datapath,str(myday.year))
                #writeChannels(cnx,d.begin,d.end,os.path.join(datapath,str(myday.year)),tags)
                pool.apply_async(process_historic_canary.zipAndLoad, args=(zipPath,d.strday, result))
        finally:
            pool.close()
            pool.join()
            cnx.close()
            while not result.empty():
                if result.get()[1]:
                    success.append(result.get()[0])
                else:
                    failed.append(result.get()[0])
    return success, failed


def writeChannels(cnx,start, stop,mypath, tags):
    '''Write all records for a list of channels between start and stop datetime to csv file
    csv files are named with the start datetime and written to the specified path.
    :param: cnx: a connection to a database permitting SQL queries with tables named channel and data
    :param start: the start datetime of records to export
    :param stop: the end datetime of records to export
    :param tags: a list of channel names to export. Each channel will generate a seperate csv file.
    '''

    cursor = cnx.cursor()
    try:
        for t in tags:
            cursor.execute(
                "SELECT channel, description,datetime,value,quality FROM data LEFT JOIN channel on data.channel = channel.name WHERE channel LIKE %s AND datetime BETWEEN %s AND %s",
                ('%.' + t, start, stop))
            records = cursor.fetchall()
            newcsvName = t.replace(".", "_") + (start.strftime(DATEFORMAT)).split(' ')[0] + ".csv"
            filepath = os.path.join(*[mypath, newcsvName])  # this is only the file prefix
            if len(records) > 0:
                process_historic_canary.writeRecords(records, filepath)
    finally:
        cursor.close()
    return

def matchChannels(ccnx,pcnx):
    '''adds new channels found in historian tag table to postgresql channel table
    :param ccnx: an open connection to historian
    :param pcnx: an open connection to postgresql'''
    cursor = ccnx.cursor()
    tags = cursor.execute("SELECT tag_name,description FROM tags WHERE tag_name like '%.Cordova.%'").fetchall()
    cursor.close()
    updateChannels(tags, pcnx)
    return

def insertTag(tag,cursor):
    cursor.execute("INSERT INTO channel(name, description) VALUES (%s, %s) ON CONFLICT DO NOTHING",tag)
    return

def updateChannels(tags,cnx):
    '''
    adds new tags to the channel table for the provided connection
    :param tags: list of tuples of the form (tag_name, tag_description)
    :param cnx: connection to the database containing channel data table
    :return:
    '''
    cursor = cnx.cursor()
    [insertTag(t,cursor) for t in tags]
    cnx.commit()
    return


def Canary2Timescale(days2Insert):
    '''Queries records from canary labs historian connection and inserts those records in a postgresql database
    The days to insert parameter provide the start time to query, records up to the end of the start day are inserted
    :param days2Insert a list of datetimes to insert into the postgresql database'''
    #connect to canary
    cnxn = connect2Canary()
    #connect to pg
    pgcnxn = connect2Pg()
    matchChannels(cnxn,pgcnxn)
    #read canary
    print(datetime.datetime.now())
    for d in days2Insert: #day is a datetime object
        myday = HistorianDay(d)
        #too much data if we pull an entire day all at once so we increment through the date.
        upHour = datetime.timedelta(hours=4)
        starttime = myday.begin
        while starttime < myday.end -upHour:
            records = readCanary(cnxn,starttime,starttime + upHour,None)
            #write pg
            if len(records) > 0:
                write2Pg(pgcnxn,records)
            starttime = starttime + upHour
    print(datetime.datetime.now())
    #cleanup
    cnxn.close()
    pgcnxn.close()
def refreshAnomolyYM(ym,cnx):
    cursor = cnx.cursor()

    try:
        refreshMaterial(ym,cnx)
    except Exception as e:
        createAnomolyView(ym,cnx)
        print(e)
    finally:
        cursor.close()
    return
def createAnomolyView(ym,cnx):
    #run on seperate thread
    return
def extractYearMonths(days):
    '''return a formatted string of unique year month combinations from a list of dates'''

    return set(["_".join([str(d.year),'{:02d}'.format(d.month)])  for d in days])

def refreshViews(daysWithNewData):
    yearMonths = extractYearMonths(daysWithNewData)
    cnx = connect2Pg()
    for ym in yearMonths:
        try:
            #try to refresh the view
             refreshMaterial(ym,cnx)
        except psycopg2.OperationalError as e:
            print(e)
            pass
        except psycopg2.ProgrammingError as e:

            #if the table did not exist create it:
            makeMaterial(ym,cnx)
    cnx.close()
def createMissingViews(daysWithNewData):
    yearMonths = extractYearMonths(daysWithNewData)
    cnx = connect2Pg()
    cursor = cnx.cursor()
    for ym in yearMonths:
        try:
            #try to refresh the view
            cursor.execute("SELECT * FROM {0} LIMIT 1;",("anomoly_" + ym))
        except psycopg2.OperationalError as e:
            print(e)
            pass
        except psycopg2.ProgrammingError as e:

            #if the table did not exist create it:
            cnx.rollback()
            makeMaterial(ym,cnx)
    cursor.close()
    cnx.close()

def makeMaterialFullYear(year=2020):
    days = [datetime.datetime(year, m, 1) for m in range(1,12)]
    createMissingViews(days)

