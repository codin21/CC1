from flask import Flask,request,Response,jsonify
from subprocess import call
import json
import os
import re
import time
import subprocess

app = Flask(__name__)


# f = open("sum.txt","a")
# def execute(cmd):
#         popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
#         for stdout_line in iter(popen.stdout.readline, ""):
#             yield stdout_line 
#         popen.stdout.close()
#         return_code = popen.wait()
#         if return_code:
#             raise subprocess.CalledProcessError(return_code, cmd)





@app.route('/send')
def postquery():
    query=request.args['query']
    print(query)
    fl='echo '+query+' > /home/hduser/input.txt'
    call(fl,shell=True)
    deleteInput='hadoop fs -rmr /user/hduser/files/input.txt'
    call(deleteInput,shell=True)
    deleteOutput='hadoop fs -rmr /user/hduser/files/output5'
    call(deleteOutput,shell=True)
    put='hdfs dfs -put /home/hduser/input.txt /user/hduser/files'
    call(put,shell=True)    
    l = query.lower()
    # print(l)  
    if "join" in l:   
        t = time.localtime()
        current_m1 = time.strftime("%M", t)
        current_s1 = time.strftime("%S", t)
        cmd = 'hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.6.0.jar -input /user/hduser/files/input.txt -mapper "/home/hduser/mapper1.py" -reducer "/home/hduser/reducer2.py" -output /user/hduser/files/output5' 
        call(cmd, shell=True)
        
        
        # cat = subprocess.Popen(["hadoop", "fs", "-cat", "/path/to/myfile"], stdout=subprocess.PIPE)
        # for line in cat.stdout:
        #     print (line, sep=' ',end='/n', file=ss.stdout, flush=False)
        
        
        t = time.localtime()
        current_m2 = time.strftime("%M", t)
        current_s2 = time.strftime("%S", t)
        hm=int(current_m2)-int(current_m1)
        hs=int(current_s2)-int(current_s1)
        if (hs<0):
            hs=hs+60
            hm=hm-1
        diff=str(hm) +" mins " + str(hs) +" secs."
        print (hm)
        print (hs)
        with open('output_reduce.json') as inFile:
            try: 
                mpap_list = json.load(inFile)
            except ValueError:
                print('rrereee')
                mpap_list = []
    
        t = time.localtime()
        current_m1 = time.strftime("%M", t)
        current_s1 = time.strftime("%S", t)
        cmd = 'spark-submit --master sparkMaster/local join.py '+query
        call(cmd, shell=True)
        t = time.localtime()
        current_m2 = time.strftime("%M", t)
        current_s2 = time.strftime("%S", t)
        hm=int(current_m2)-int(current_m1)
        hs=int(current_s2)-int(current_s1)
        if (hs<0):
            hs=hs+60
            hm=hm-1
        diff_s=str(hm) +" mins " + str(hs) +" secs."
        print (hm)
        print (hs)
        with open('outputJoin1.json') as inFile:
            try: 
                spark_list = json.load(inFile)
            except ValueError:
                print('rrereee')
                spark_list = []
    
    if "group" in l: 
        t = time.localtime()
        current_m1 = time.strftime("%M", t)
        current_s1 = time.strftime("%S", t)
        cmd = 'hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.6.0.jar -input /user/hduser/files/input.txt -mapper "/home/hduser/mapper.py" -reducer "/home/hduser/reduce.py" -output /user/hduser/files/output5' 
        call(cmd, shell=True)
        

        # cat = subprocess.Popen(["hadoop", "fs", "-cat", "/path/to/myfile"], stdout=subprocess.PIPE)
        # for line in cat.stdout:
        #     print (line, sep=' ',end='/n', file=ss.stdout, flush=False)
        
        
        t = time.localtime()
        current_m2 = time.strftime("%M", t)
        current_s2 = time.strftime("%S", t)
        hm=int(current_m2)-int(current_m1)
        hs=int(current_s2)-int(current_s1)
        if (hs<0):
            hs=hs+60
            hm=hm-1
        diff=str(hm) +" mins " + str(hs) +" secs."
        print (hm)
        print (hs)
        with open('outputgroupby.json') as inFile:
            try: 
                mpap_list = json.load(inFile)
            except ValueError:
                print('rrereee')
                mpap_list = []
        


        t = time.localtime()
        current_m2 = time.strftime("%M", t)
        current_s2 = time.strftime("%S", t)
        cmd = 'spark-submit --master sparkMaster/local group.py '+query
        call(cmd, shell=True)
        t = time.localtime()
        current_m2 = time.strftime("%M", t)
        current_s2 = time.strftime("%S", t)
        hm=int(current_m2)-int(current_m1)
        hs=int(current_s2)-int(current_s1)
        if (hs<0):
            hs=hs+60
            hm=hm-1
        diff_s=str(hm) +" mins " + str(hs) +" secs."
        print (hm)
        print (hs)
        with open('output11.json') as inFile:
            try: 
                spark_list = json.load(inFile)
            except ValueError:
                print('rrereee')
                spark_list = []
    
    

    return jsonify(mrresult=mpap_list,mr_time=diff, sparkoutput=spark_list, spark_time=diff_s),200


if __name__=="__main__":
    app.run(debug=True)

    