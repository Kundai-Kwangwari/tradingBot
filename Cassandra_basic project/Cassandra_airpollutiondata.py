from cassandra.cluster import Cluster
from cassandra.io.libevreactor import LibevConnection
from cassandra.auth import PlainTextAuthProvider
from zipfile import ZipFile
import os
import glob

import re

cluster = Cluster(["10.0.0.105"])
auth_provider = PlainTextAuthProvider(username='hadoopuser', password='*********')
#cluster.connection_class = LibevConnection
session = cluster.connect('cassandrateam')
print(session)
print("using keyspace CAssandrateam...")
session.execute ('USE cassandrateam;')
print('creating table US_airpollution')
session.execute('create table if not exists testfinal ( StateCode int, CountryCode int, SiteNum int, ParameterCode int, POC int, Latitude float, Longitude float, Datum varchar, ParameterName varchar, DateLocal varchar, TimeLocal varchar, DateGMT varchar, TimeGMT varchar, SampleMeasurement int, UnitsOfMeasure varchar, MDL int, Uncertainity varchar, Qualifier varchar, MethodType varchar, MethodCode int, MethodName varchar, StateName varchar, CountryName varchar, DateOfLastChange varchar, PRIMARY KEY(StateCode, CountryCode, SiteNum, DateLocal));')


f1 = "/home/hadoopmaster/Downloads/cassandraData/US_airPollution_PM25(1).zip"
f2 = "/home/hadoopmaster/Downloads/cassandraData/usdata"
f3 = "/home/hadoopmaster/Downloads/cassandraData"


file_paths = []
extension = extension = 'csv'
for root,directories,files in os.walk(f3):
    os.chdir(f2)
    file_names = [i for i in glob.glob('*.{}'.format(extension))]
    for filename in file_names:
        filepaths = os.path.join(f2, filename)
        #print(filepaths)
        file_paths.append(filepaths)

count = 0
for file in file_paths:
    print('processing file: ',file)
    sourceFile = open('/home/hadoopmaster/cassandraNotes/cassandraProcessingListF.txt', 'a')
    sourceFile.write(file)
    sourceFile.close()
    if count == 0:
       count += 1
       session.execute("copy testfinal (StateCode , CountryCode , SiteNum , ParameterCode ,POC , Latitude ,Longitude ,Datum, ParameterName , DateLocal , TimeLocal ,DateGMT,TimeGMT ,SampleMeasurement ,UnitsOfMeasure, MDL , Uncertainity , Qualifier , MethodType , MethodCode , MethodName , StateName , CountryName , DateOfLastChange ) from 'file' with header = true and delimiter = ',';")
    '''
    session.execute("copy testfinal (StateCode , CountryCode , SiteNum , ParameterCode ,POC , Latitude ,Longitude ,Datum, ParameterName , DateLocal , TimeLocal ,DateGMT,TimeGMT ,SampleMeasurement ,UnitsOfMeasure, MDL , Uncertainity , Qualifier , MethodType , MethodCode , MethodName , StateName , CountryName , DateOfLastChange ) from 'file' with header = false and delimiter = ',';")
    '''
    with open(file, "r") as database:
        count = 0
        non_decimal = re.compile(r'[^\d.]+')
        for transaction in database:
            if count == 0:
               count += 1
               print("Header is:\n",transaction.split(","))
               continue
            count += 1
            column = transaction.split(",")
            print(column, "line is:", count ,"file is :",file)
            StateCode = int(float(non_decimal.sub('',column[0]))) if column[0]!='' else 0
            CountryCode = int(float(non_decimal.sub('',column[1]))) if column[1]!='' else 0
            SiteNum = int(float(non_decimal.sub('',column[2]))) if column[2]!='' else 0
            ParameterCode = int(float(non_decimal.sub('',column[3]))) if column[3] !='' else 0
            POC =  int(float(non_decimal.sub('',column[4]))) if column[4]!='' else 0
            Latitude =float(non_decimal.sub('',column[5])) if column[5]!='' else 0
            Longitude = float(non_decimal.sub('',column[6])) if column[6]!='' else 0
            Datum = column[7]
            ParameterName = column[8]
            #DateLocal = datetime.strptime(column[9], "%d-%m-%Y").date()
            DateLocal = column[9]
            TimeLocal = column[10]
            DateGMT = column[11]
            TimeGMT = column[12]
            SampleMeasurement = int(float(non_decimal.sub('',column[13]))) if column[13]!='' else 0
            UnitsOFMeasure = column[14]
            MDL = int(float(non_decimal.sub('',column[15]))) if column[15]!='' else 0
            Uncertainity = column[16]
            Qualifier = column[17]
            MethodType = column[18]
            MethodCode = int(float(non_decimal.sub('',column[19]))) if column[19]!='' else 0
            MethodName = column[20]
            StateName = column[21]
            CountryName = column[22]
            DateOfLastChange = re.sub("[\\n]",'',column[23])

            session.execute("insert into test(StateCode , CountryCode , SiteNum , ParameterCode ,POC , Latitude ,Longitude ,Datum, ParameterName , DateLocal , TimeLocal ,DateGMT,TimeGMT ,SampleMeasurement ,UnitsOfMeasure, MDL , Uncertainity , Qualifier , MethodType , MethodCode , MethodName , StateName , CountryName , DateOfLastChange )values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(StateCode, CountryCode, SiteNum, ParameterCode, POC, Latitude, Longitude, Datum, ParameterName, DateLocal, TimeLocal, DateGMT, TimeGMT, SampleMeasurement,UnitsOFMeasure ,MDL, Uncertainity, Qualifier, MethodType, MethodCode, MethodName, StateName, CountryName, DateOfLastChange))



session.shutdown()
cluster.shutdown()
