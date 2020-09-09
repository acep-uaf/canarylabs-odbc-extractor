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

DATEFORMAT = "%Y-%m-%d %H:%M:%S.%f"

class day:
    def __init__(self,d):
        self.begin = datetime.datetime.combine(d, datetime.datetime.min.time())
        self.end = datetime.datetime.combine(d, datetime.datetime.max.time())


def readPGconfig():
    "read a configuration file to retrieve access token"
    configDict = {}
    with open(os.path.join(*['instance','pgconfig.cfg']), 'r') as config:
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
    logFile = open(datetime.datetime.today().strftime("%Y-%m-%d") + 'log.txt', "w+")
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
    # cursor.execute("""INSERT INTO data(datetime,channel,value,quality)
    # SELECT datetime, channel,value,quality FROM tmp WHERE datetime || channel NOT IN (SELECT datetime || channel FROM data)""")
    #cursor.execute("""INSERT INTO data(datetime,channel,value,quality)
    #    SELECT datetime, channel,value,quality FROM tmp ON CONFLICT DO NOTHING""")
    #cursor.execute("DELETE FROM tmp")
    #cnx.commit()
    cursor.close()
    return
def getAllTags(start,stop):
    '''
    :param: start datetime
    :param: stop datetime
    '''
    cnx = connect2Pg()
    cursor = cnx.cursor()
    tags = []
    start = start.strftime(DATEFORMAT)
    stop = stop.strftime(DATEFORMAT)
    try:
        cursor.execute(
                "SELECT UNIQUE channel FROM data WHERE datetime BETWEEN " + start + " AND " + stop)
        tagtuples = cursor.fetchall()
        tags = [".".join(t[0].split(".")[1:]) for t in tagtuples]

    finally:
        cursor.close()
        cnx.close()
    return tags
def writeData(tags,filedays,datapath):
    cnx = connect2Pg()
    try:
        if tags is None:
            tags = getAllTags(min(filedays),max(filedays))
        for t in tags:
            for myday in filedays:
                d = day(myday)
                writeChannels(cnx,d.begin,d.end,os.path.join(datapath,str(myday.year)),tags)
    finally:
        cnx.close()



def writeChannels(cnx,start, stop,mypath, tags):
    '''Write all records for the specified channel between start and stop datetime to csv file
    csv files are named with the start datetime.
    If a record was previously exported it will not be exported again'''

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

def matchChannels(ccnx,pcnx):
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
    # start = getLastInsert(pgcnxn) #start needs to be a datetime object
    # minFileDateTime = minFileDateTime.replace(tzinfo=pytz.timezone('US/Alaska'))
    # if ((start is None) or  (start[0] > minFileDateTime)):
    #     #if a file was newly downloaded but from an earlier date than the current database state go back and re-upload based on the file date.
    #     start = minFileDateTime
    #
    # stop = datetime.datetime.today()
    matchChannels(cnxn,pgcnxn)
    #read canary
    print(datetime.datetime.now())
    for d in days2Insert: #day is a datetime object
        myday = day(d)
        #too much data if we pull an entire day all at once.
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