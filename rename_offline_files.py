import os
os.chdir("D:\\Historian Data\\Cordova2019")
files = os.listdir()
for file in files:
    if file[-3:] == 'off':
        os.rename(file,file[:-4])
    
