# canarylabs-odbc-extractor

Running AutoProcess.main() or runProcessing.bat will pull new canary files from the specified dropbox into the data folder for Historian. 
Once Historian recognizes the new files the process_historic_canary.main() they will be exported into csv files containing 24 hours of data for each channel
in the Historian database. The individuals files are zipped by day and uploaded to the specified Box folder. This process runs automatically once
every 24 hours, but new csv files are only generated once new data has been placed in the dropbox folder. 
