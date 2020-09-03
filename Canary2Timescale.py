# Projet: canarylabsdataextractor
# Created by: # Created on: 9/3/2020
# Purpose :  Canary2Timescale
import os
import psycopg2
import datetime
import pyodbc
#CONSTANTS
from process_historic_canary import writeRecords

DATEFORMAT = "%Y-%m-%d %H:%M:%S.%f"

class day:
    def __init__(self,d):
        self.begin = datetime.datetime(d + '00:00:00.0001')
        self.end =  datetime.datetime(d + '11:59:59.9999')

def readPGconfig():
    "read a configuration file to retrieve access token"
    configDict = {}
    with open(os.path.join(['instance','dbconfig.cfg']), 'r') as config:
        for line in config.readlines():
            try:
                configDict[line.split("=")[0]] = line.split("=")[1].rstrip()
            except:
                pass
    return configDict
def connect2Canary():
    return pyodbc.connect('DSN=CanaryODBCClient;')
def connect2Pg():
    constring = "dbname = cordova_scada_data user=" + str(readPGconfig()['user']) +  "password=" + str(readPGconfig()['pswd'])
    return psycopg2.connect(constring)
def readCanary(cnxn,start,stop,tag):
    cursor = cnxn.cursor()
    if tag is None:
        records = cursor.execute(
            "SELECT tag_name, description, time_stamp, value, quality from data WHERE time_stamp > '" + start.strftime(
                DATEFORMAT) + "'AND time_stamp < '" + stop.strftime(DATEFORMAT) + "'").fetchall()
    else:
        records = cursor.execute(
            "SELECT tag_name, description, time_stamp, value, quality from data WHERE time_stamp > '" + start.strftime(
                DATEFORMAT) + "'AND time_stamp < '" + stop.strftime(DATEFORMAT) + "' and tag_name = '" + tag[0] + "'").fetchall()
    return records

def daterange(date1, date2):
    for n in range(int((date2 - date1).days) + 1):
        yield date1 + datetime.timedelta(n)

def getLastInsert(cnx):
    cursor = cnx.cursor()
    last = cursor.execute("SELECT max(datetime) from data").fetchall()
    cursor.close()
    return last
def write2Pg(cnx,records):
    '''

    :param cnx: connection to postgresql database
    :param records: list of tuples with the format datetime, channel, value, quality
    :return: None
    '''
    cursor = cnx.cursor()
    cursor.executemany("INSERT INTO tmp(datetime,channel,value,quality) VALUES(%s,%s,%s,%s)",records)
    cnx.commit()
    # cursor.execute("""INSERT INTO data(datetime,channel,value,quality)
    # SELECT datetime, channel,value,quality FROM tmp WHERE datetime || channel NOT IN (SELECT datetime || channel FROM data)""")
    cursor.execute("""INSERT INTO data(datetime,channel,value,quality) 
        SELECT datetime, channel,value,quality FROM tmp ON CONFLICT DO NOTHING""")
    cursor.execute("DELETE * FROM tmp")
    cnx.commit()
    cursor.close()
    return
def getAllTags(start,stop):
    cnx = connect2Pg()
    cursor = cnx.cursor()
    tags = []
    try:
        cursor.execute(
                "SELECT UNIQUE channel FROM data WHERE datetime BETWEEN " + start + " AND " + stop)
        tagtuples = cursor.fetchall()
        tags = [t[0] for t in tagtuples]

    finally:
        cursor.close()
        cnx.close()
    return tags
def writeChannels(start, stop,path, tags):
    cnx = connect2Pg()
    cursor = cnx.cursor()
    if tags is None:
        tags = getAllTags()
    try:
        for t in tags:
            records = cursor.execute(
                "SELECT channel, description,datetime,value,quality FROM data LEFT JOIN channels on data.channel = channels.name WHERE channel = '" + t + "' AND datetime BETWEEN " + start + " AND " + stop)
            newcsvName = t[0].replace(".", "_") + (start.strftime(DATEFORMAT)).split(' ')[0] + ".csv"
            filepath = os.path.join(*[path, newcsvName])  # this is only the file prefix
            if len(records) > 0:
                writeRecords(records, filepath)
    finally:
        cursor.close()
        cnx.close()
def matchChannels(ccnx,pcnx):
    cursor = ccnx.cursor()
    tags = cursor.execute("SELECT * FROM tags WHERE tag_name like '%.Cordova.%'").fetchall()
    cursor.close()
    updateChannels(tags, pcnx)

    return
def insertTag(tag,cursor):

    cursor.execute("INSERT INTO channel(name, description) VALUES (%s, %s) ON CONFLICT DO NOTHING",tag)
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


def Canary2Timescale(minFileDateTime):
    #connect to canary
    cnxn = connect2Canary()
    #connect to pg
    pgcnxn = connect2Pg()
    start = getLastInsert(pgcnxn) #start needs to be a datetime object
    if start > minFileDateTime:
        start = minFileDateTime

    stop = datetime.datetime.today()

    matchChannels(cnxn,pgcnxn)
    #read canary
    for d in daterange(datetime.date(start),stop): #day is a datetime object
        myday = day(d)
        records = readCanary(cnxn,myday.begin,myday.stop,None)
        #write pg
        if records != None:
           write2Pg(pgcnxn,records)

    #cleanup
    cnxn.close()
    pgcnxn.close()