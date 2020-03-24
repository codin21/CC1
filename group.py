import re
import sys
import json
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext, Row
APP_NAME="First groupby"
conf = SparkConf().setAppName(APP_NAME)
conf = conf.setMaster("local[*]")
sc   = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
test_string=sys.argv[1]
#test_string = "SELECT gender, min(age) FROM users GROUP BY gender HAVING min(age) < 20"
res = re.split(r'[,  \s]\s*',test_string)
col1=res[1]
#col2=res[2]
agg=res[2]
table=res[4]
op=res[10]
value=res[11]
cond=re.split(r'[()\s]\s*',agg)
func=cond[0]
a=cond[1]
t1=sc.textFile("file://///home/hduser/Desktop/HEADERcsv/"+table+".csv")
header_t=t1.first()
h=header_t.replace('"','').split(",")
c1=h.index(col1)
#c2=h.index(col2)
ag=h.index(a)
t=t1.filter(lambda line:line!=header_t).map(lambda x:x.split(","))

if(op=="="):
    op="=="

def minFun(x,y):
    if(x<y):
       return x
    else:
       return y
def maxFun(x,y):
   if(x>y):
      return x
   else:
      return y
def sumFun(x,y):
   return x+y

if(func=="min"):
   key_t=t.map(lambda line:(line[c1],line[ag])).reduceByKey(lambda x,y:minFun(x,y))
elif(func=="max"):
     key_t=t.map(lambda line:(line[c1],line[ag])).reduceByKey(lambda x,y:maxFun(x,y))
elif(func=="sum"):
     key_t=t.map(lambda line:(line[c1],line[ag])).reduceByKey(lambda x,y:sumFun(x,y))
else:
    key_t=t.map(lambda line:(line[c1],1)).reduceByKey(lambda x,y:sumFun(x,y))

def fun(line):
   l=line[1]
   e=str(l)+""+op+""+value
   return eval(e)

final=key_t.filter(lambda line: fun(line))
#schemaPeople = sqlContext.createDataFrame(final)
#schemaPeople.write.format("json","UTF-8").save("file://///home/surbhi/Code/Cloud Computing/data.json")
dlist=[]
dist={"result":[]}
for i in final.collect():
    dict={}
    dict.update( {col1 : i[0],agg:i[1]} )
    dlist.append(dict)
dist['result']=dlist

with open("/home/hduser/output11.json", "w") as outfile:
    json.dump(dist, outfile)     
    outfile.close()
#final.saveAsTextFile("file://///home/surbhi/Code/Cloud Computing/output_group.txt")
