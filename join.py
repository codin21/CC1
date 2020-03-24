import sys
import re
import json
from pyspark import SparkContext,SparkConf
import operator
APP_NAME="First Join"
conf = SparkConf().setAppName(APP_NAME)
conf = conf.setMaster("local[*]")
sc   = SparkContext(conf=conf)
test_string=sys.argv[1]
#test_string = "SELECT * FROM users INNER JOIN zipcodes ON users.zipcode=zipcodes.zipcode WHERE city='DURHAM'"
#test_string = "SELECT * FROM users INNER JOIN rating ON users.userid = rating.userid WHERE occupation = "none""
res = test_string.split()
table1=res[3]
table2=res[6]
on=res[8].split(".")[1]
#whereCond=res[10]
#on=re.split(r'[.=\s]\s*',onCond)
#wh=re.split(r'[< = >\s]\s*',whereCond)
column=res[12]
value=res[14]
op=res[13]
if(op=="="):
   op="=="
#if(whereCond.find(">")):
#   op=">"
#if(whereCond.find("<")):
#   op="<"
t1=sc.textFile("file://///home/hduser/Desktop/HEADERcsv/"+table1+".csv")
t2=sc.textFile("file://///home/hduser/Desktop/HEADERcsv/"+table2+".csv")

#Filter the header 
header_t1=t1.first()
header_t2=t2.first()

tt1=header_t1.replace('"','')
tt2=header_t2.replace('"','')

h1=tt1.split(",")
h2=tt2.split(",")

#index of on condition clause 
i1=h1.index(on)
i2=h2.index(on)

#find out the index of the column on which condition is to be applied
try:
  c=h1.index(column)
  frm=1
except:
  c=h2.index(column) 
  frm=2

#update the index column of where
if(frm==2):
   c=len(h1)+c

#remove the header of csv file
new_t1=t1.filter(lambda line:line!=header_t1)
new_t2=t2.filter(lambda line:line!=header_t2)

#delimiter removal & key value pair 
rdd_t1=new_t1.map(lambda x:x.split(",")).map(lambda line: (line[i1],line))
rdd_t2=new_t2.map(lambda x:x.split(",")).map(lambda line: (line[i2],line))

#join condition
result=rdd_t1.join(rdd_t2)

#map to get the result
r=result.map(lambda line: (line[1][0]+line[1][1]))

#where condition
"""
def fun(line):
   l=line[c]
   e=l+""+op+""+value
   return eval(e)
"""
def fun(line):
   l=line[c]
   e=operator.eq(l,value)
   return e

final=r.filter(lambda line: fun(line))
l1=len(h1)
l2=len(h2)
dlist=[]
dist={"result":[]}
for i in final.collect():
    dict={}
    j=0 
    k=0
    while j<l1:
        dict.update( {h1[j] : i[j]})
        j=j+1
    while k<l2:
        dict.update( {h2[k] : i[l1+k]})
        k=k+1    
    dlist.append(dict)
dist['result']=dlist

with open("/home/hduser/outputJoin1.json", "w") as outfile:
    json.dump(dist, outfile)     
    outfile.close()

#final.saveAsTextFile("file://///home/surbhi/Code/Cloud Computing/output_join.txt")
