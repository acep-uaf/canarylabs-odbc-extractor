import datetime


def getEnd(d):
    if d.month != 12:
        return datetime.datetime(d.year, d.month + 1, 1) - datetime.timedelta(days=1)
    else:
        return datetime.datetime(d.year +1, 1, 1) - datetime.timedelta(days=1)

def refreshMaterial(ym,cnx):
    viewName = getViewName(ym)
    sqlstr = "REFRESH MATERIALIZED VIEW {0};".format(viewName)
    submitSQL(sqlstr,cnx)
    return

def submitSQL(sqlstr,cnx):
    cursor = cnx.cursor()
    try:
        cursor.execute(sqlstr)
        cnx.commit()
    except Exception as e:
        print(e)
        print(sqlstr)
        cnx.rollback()
        raise
    finally:
        cursor.close()
    return
def getViewName(ym):
    return 'anomaly_{0}'.format(ym)
def getViewStart(ym):
    '''get the start datetime assocaited with a year-month combo
    :param ym is a string formatted as yyyy-mm'''
    print(ym)
    start = datetime.date(int(ym[0:4]), int(ym[6:8]), 1)
    start = datetime.datetime.combine(start, datetime.datetime.min.time())
    return start
def getViewEnd(ym):
    start = getViewStart(ym)
    end = datetime.datetime.combine(getEnd(start), datetime.datetime.max.time())
    return end

def makeMaterial(ym, cnx):
        viewName = getViewName(ym)
        sqlstr = """CREATE MATERIALIZED VIEW {0} AS
        WITH 
        weekly as(
        SELECT channel, date_trunc('week',datetime::date) as tm, avg(value::numeric) as ave, 
            percentile_cont(0.5) WITHIN group (order by value::numeric) as med, 
            percentile_cont(0.1) WITHIN group (order by value::numeric) as p10,
            percentile_cont(0.9) WITHIN group (order by value::numeric) as p90 
            FROM data 
        
        WHERE value ~ E'^[+-]?[0-9]+(\\\\.[0-9]*)?([Ee][+-]?[0-9]+)?\$' AND datetime BETWEEN '{1}' AND '{2}'
        GROUP BY channel,date_trunc('week',datetime::date) ORDER BY channel, date_trunc('week',datetime::date))
        
        
        SELECT data.channel, datetime, value, ave, med,p90,p10 FROM data 
        INNER JOIN weekly on date_trunc('week',data.datetime::date) = weekly.tm::date AND data.channel = weekly.channel
        GROUP BY data.channel, datetime, data.value, ave, med, p90,p10;""".format(viewName,str(getViewStart(ym)),str(getViewEnd(ym)))

        submitSQL(sqlstr,cnx)
