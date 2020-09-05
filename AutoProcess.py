import datetime
import os
import process_historic_canary

#from process_historic_canary import writeLast

logPath = 'C:\\Users\\radiance-admin\Canary_Labs_Data_Management\canarylabs-odbc-extractor'
logFile = 'ScheduledTasksLog.txt'
log =os.path.join(logPath,logFile)

def logRuns():
  writeToLog('RunStart' + datetime.datetime.now().strftime("%Y-%m-%d %H%M%S"))
  return
def logComplete():
  writeToLog('RunComplete' + datetime.datetime.now().strftime("%Y-%m-%d %H%M%S"))
  return

def writeToLog(msg):
    with open(log, "a+") as logFile:
        logFile.write('\n'  + msg)
    return

def logFiles(dboxCount, zipCount,failedCount):
  writeToLog('Dropbox files transferred:' + str(dboxCount))
  writeToLog('zipFiles uploaded:' + str(zipCount))
  writeToLog('zipFiles not uploaded:' + str(failedCount))
  return

def main():
    logRuns()
    try:
        result= process_historic_canary.main()
        logFiles(result['downloaded'],result['uploaded'],result['failedUploads'])
    # except ProcessingStoppedError as e:
    #     writeLast(e.datetime)
    #     writeToLog(e.message)
    except Exception as e:
        print(e)
        writeToLog(str(e))
    logComplete()
    return

if __name__ =='__main__':
    main()
