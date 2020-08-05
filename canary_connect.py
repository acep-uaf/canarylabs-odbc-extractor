import pyodbc
import csv
'''This script reads rows of data from a Canary Labs Historian Database and writes the raw data to a
csv file in the same format it is found in the database. It can be used to test a connection to a
database and extract simple data.
In addition to this script a text file named lastRecord.txt is required.
'''
#Text file containing the timestamp of the last record to go back to
#i.e. if you wanted everything back to June 09, 2019 you would put 2019-06-30 00:27:00
#This datetimestamp will get overwritten with the dateime of the
#last record extracted once the script completes.

lastPath ="lastRecord.txt"
try:
    lastfile = open(lastPath, 'r')
    last = lastfile.readline() #The variable to store the last datetimestamp
    lastfile.close() #close the file
except Exception as e:
    print(e)
    last = '2017-06-30 00:00:00'
    
print('Retrieving data back to %l' %last)

#The connection to the database
cnxn = pyodbc.connect('DSN=CanaryODBCClient;')
cursor = cnxn.cursor()
#Create a list of tuple records from the data table
records = cursor.execute("SELECT * from data WHERE time_stamp > ? ORDER BY time_stamp",str(last)).fetchall()
print(" %r records found " %len(records)) #the number of records found is printed to the screen

#write the datetimestamp of the newest record found to the lastfile.
#This will be used for reference the next time the script is run
lastfile = open(lastPath, 'w')
lastfile.write(str(records[len(records)-1][2]))
lastfile.close()

#Name the csv file to include the newest timestamp value extracted
csvName = "data" + str(records[len(records)-1][2]).replace(":","_")+ ".csv"

#write the file to the same directory the script is stored           
with open(csvName, mode='w+') as Mycsv:
    csv_writer = csv.writer(Mycsv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(['tag_name','description','datetime_stamp','value','quality'])
    for row in records:
        csv_writer.writerow(row)
   

    
    
