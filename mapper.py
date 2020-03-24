#!/usr/bin/python3.5
import sys
import re
import csv
#import os
line=sys.stdin.readline()
line=line.strip()
res = re.split(r'[,  \s]\s*',line)

col1=res[1]
#col2=res[2]
agg=res[2]
table=res[4]
op=res[10]
value=res[11]
cond=re.split(r'[()\s]\s*',agg)
func=cond[0]
a=cond[1]
rows=[]
filename="/home/hduser/Desktop/HEADERcsv/"+table+".csv"
#filename="C:\Users\lenovo\Downloads\CC_Ass1_dataset"+table+".csv"
#v=os.system('table')

with open(filename, 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    fields = next(csvreader)
    for row in csvreader: 
        rows.append(row)
c1=fields.index(col1)
#c2=fields.index(col2)
ag=fields.index(a)
#fu=fields.index(func)
b=list(map(lambda x:(x[c1],str(x[ag])+'_'+func+'_'+op+'_'+value+'_'+a+'_'+col1),rows))

#print ("col1="+col1)
#print ("agg="+agg)
#print ("table="+table)
#print ("op="+op)
#print ("value="+value)
#print ("func="+func)
#print ("a="+a)
for e in b:
    print ('%s\t%s' % (e[0],e[1]))
