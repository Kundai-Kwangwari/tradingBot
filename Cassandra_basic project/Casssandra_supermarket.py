from cassandra.cluster import Cluster
from cassandra.io.libevreactor import LibevConnection
from cassandra.auth import PlainTextAuthProvider
from zipfile import ZipFile
import os, psutil
import glob
import pandas as pd
import csv
import datetime
import re

cluster = Cluster(["10.0.0.105"])
auth_provider = PlainTextAuthProvider(username='hadoopuser', password='hadoopmaster')
session = cluster.connect('cassandrateam')
print(session)
print('creating table newretail...')
session.execute("create table if not exists supermarket (transactionDT varchar, customer_id int, AGE_group varchar, pincode varchar, product_subclass int, product_id varchar, amount int, asset int, sales_price int, primary key(customer_id));")

print("Inserting data into newretail...")
f1 = "/home/hadoopmaster/Downloads/cassandraData/US_airPollution_PM25(1).zip"
f2 = "/home/hadoopmaster/Downloads/cassandraData/retaildata"
f3 = "/home/hadoopmaster/Downloads/cassandraData"

print("extracting paths....")
os.chdir(f2)
file_paths = []
extension = extension = 'csv'
for root,directories,files in os.walk(f2):
    file_names = [i for i in glob.glob('*.{}'.format(extension))]
    for filename in file_names:
        filepaths = os.path.join(f2, filename)
        file_paths.append(filepaths)

print(file_paths)

for file in file_paths:
    print('processing file: ',file)
    sourceFile = open('/home/hadoopmaster/cassandraNotes/cassandraProcessingListF.txt', 'a')
    sourceFile.write(file)
    sourceFile.close()
    with open(file, "r") as database:
         count = 0
         non_decimal = re.compile(r'[^\d.]+')
         for transaction in database:
            if count == 0:
               count += 1
               # print("Header is:\n",transaction.split(","))
               continue
            count += 1
            column = transaction.split(",")
            print("line is:", count, "file is: ", file,"  |",column)
            transactionDT = column[0]
            customer_id = int(float(non_decimal.sub('',column[1]))) if column[1]!='' else 0
            AGE_group = column[2]
            pincode =  column[3]
            product_subclass = int(float(non_decimal.sub('',column[4]))) if column[4]!='' else 0
            # product_id  = int(float(non_decimal.sub('',column[5]))) if column[5]!='' else 0
            product_id  = column[5]
            amount = int(float(non_decimal.sub('',column[6]))) if column[6]!='' else 0
            asset = int(float(non_decimal.sub('',column[7]))) if column[7]!='' else 0
            sales_price = int(float(non_decimal.sub('',column[8]))) if column[8]!='' else 0

            session.execute("insert into supermarket (transactionDT , customer_id , AGE_group , pincode , product_subclass , product_id , amount , asset , sales_price )values(%s,%s,%s,%s,%s,%s,%s,%s,%s)",(transactionDT , customer_id , AGE_group , pincode , product_subclass , product_id , amount , asset , sales_price ))

session.shutdown
cluster.shutdown()
