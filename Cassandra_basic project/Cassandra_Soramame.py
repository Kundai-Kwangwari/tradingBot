from cassandra.cluster import Cluster
from cassandra.io.libevreactor import LibevConnection
from cassandra.auth import PlainTextAuthProvider
from zipfile import ZipFile
import os
import re

cluster = Cluster(["10.0.0.105"])
auth_provider = PlainTextAuthProvider(username='hadoopuser', password='**********')
session = cluster.connect('cassandrateam')
print(session)
print("using keyspace CAssandrateam...")
session.execute ('USE cassandrateam;')


print('creating table soramame...')
session.execute("create table if not exists soramame (code int, Date varchar, hour int, SO2 float, NO float, NO2 float, NOx float,CO float, Ox float, NMHC FLOAT, CH4 FLOAT, THC FLOAT, SPM float, PM2_5 float,SP float,WD varchar, WS float, TEMP float, HUM float,Primary key(code));")

print("Inserting data into soramame...")

f1 = "/home/hadoopmaster/Downloads/cassandraData/US_airPollution_PM25(1).zip"
f2 = "/home/hadoopmaster/Downloads/cassandraData/soramame/data/"
f3 = "/home/hadoopmaster/Downloads/cassandraData/soramame/data"


#os.chdir(f2)
file_paths = []
extension = extension = 'csv'
for root,directories,files in os.walk(f3):
    for filename in files:
        filepaths = os.path.join(root, filename)
        if filepaths.endswith(extension):
           file_paths.append(filepaths)




sourceFile = open('/home/hadoopmaster/cassandraNotes/cassandraProcessingListF.txt', 'r')
#sourceFile.write(file_paths)
done = sourceFile.readlines()
sourceFile.close()


for i in range(len(done)):
    done[i]= done[i].replace("\n","")


fin = ['1']
non_decimal = re.compile(r'[^\d.]+')
for file in file_paths:
   if file not in done:
     print(file)
     with open(file, "r") as database:
       # print('file not in done')
        count = 0
        non_decimal = re.compile(r'[^\d.]+')
        for transaction in database:
           if count == 0:
               count += 1
               continue
           count += 1
           column = transaction.split(",")
           print(column, "line is:", count ,"file is :",file)
           code = int(float(non_decimal.sub('',column[0]))) if column[0]!='' else 0
           Date = column[1]
           hour = int(float(non_decimal.sub('',column[2]))) if column[2]!='' else 0
           SO2 = float(non_decimal.sub('',column[3])) if column[3]!='' else 0
           NO = float(non_decimal.sub('',column[4])) if column[4]!='' else 0
           NO2 = float(non_decimal.sub('',column[5])) if column[5]!='' else 0
           NOx = float(non_decimal.sub('',column[6])) if column[6]!='' else 0
           CO = float(non_decimal.sub('',column[7])) if column[7]!='' else 0
           Ox = float(non_decimal.sub('',column[8])) if column[8]!='' else 0
           NMHC = float(non_decimal.sub('',column[9])) if column[9]!='' else 0
           CH4 = float(non_decimal.sub('',column[10])) if column[10]!='' else 0
           THC = float(non_decimal.sub('',column[11])) if column[11]!='' else 0
           SPM = float(non_decimal.sub('',column[12])) if column[12]!='' else 0
           PM2_5 = float(non_decimal.sub('',column[13])) if column[13]!='' else 0
           SP = float(non_decimal.sub('',column[14])) if column[14]!='' else 0
           WD = column[15]
           WS = float(non_decimal.sub('',column[16])) if column[16]!='' else 0
           TEMP = float(non_decimal.sub('',column[17])) if column[17]!='' else 0
           HUM = 0 if column[18] =='\n' else float(non_decimal.sub('',column[18]))


           session.execute("insert into soramame(code , Date, hour,  SO2 , NO , NO2 , NOx ,CO , Ox , NMHC , CH4 , THC , SPM , PM2_5 ,SP ,WD , WS , TEMP , HUM )values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(code , Date, hour,  SO2 , NO , NO2 , NOx ,CO , Ox , NMHC , CH4 , THC , SPM , PM2_5 ,SP ,WD , WS , TEMP , HUM ))

     if file not in fin:
              sourceFile = open('/home/hadoopmaster/cassandraNotes/cassandraProcessingListF.txt', 'a')
              #sourceFile.write(file_paths)
              sourceFile.write(file)
              fin.append(file)
              sourceFile.write("\n")
              print("file not in fin", file)
              sourceFile.close()

session.shutdown()
cluster.shutdown()









