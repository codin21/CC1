#!/usr/bin/python3.5
import re
import json
import csv
import sys
    
line=sys.stdin.readline()
line=line.strip()
data = re.split("\s", line)
table, cond1 = data[8].split(".")

resultj = {}
resultj['data'] = []
table1 = data[3]
x=open("/home/hduser/Desktop/WITHOUTHEADERCSV/"+table1+".csv")
y=csv.reader(x)
z=[]
for row in y:
    z.append(row)
newlist=[]
for n in z:
    n = [table1] + n
    str1 = ','.join(str(e) for e in n)
    newlist.append(str1)
    
table2 = data[6]
x=open("/home/hduser/Desktop/WITHOUTHEADERCSV/"+table2+".csv") 
y=csv.reader(x)
z=[]
for row in y:
    z.append(row)
for n in z:
    n = [table2] + n
    str1 = ','.join(str(e) for e in n)
    newlist.append(str1)
for record in newlist:
    if table1 in record:
        table, userid, age, gender, occupation, zipcode = record.strip().split(",")
        print (userid + "\t" + table, age, gender, occupation, zipcode)
        resultj['data'].append({'userid': userid, 'table': table, 'age': age, 'gender': gender, 'occupation': occupation, 'zipcode': zipcode})
    elif table2 in record:
        table, userid, movieid, rating, timestamp = record.strip().split(",")
        print (userid + "\t" + table, movieid, rating, timestamp)
        resultj['data'].append({'userid': userid, 'table': table, 'movieid': movieid, 'rating': rating, 'timestamp': timestamp})
with open('/home/hduser/Desktop/output_mapper.json', 'w') as outfile:
    json.dump(resultj, outfile)     
