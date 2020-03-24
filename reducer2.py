#!/usr/bin/python3.5
from operator import itemgetter
import sys
import json

intermediate = {}
resultj = {}
resultj['data'] = []

result = []
key = None
list_of_values = []
list_of_values1 = []
values = []
for line in sys.stdin:
    line = line.strip()
    key, list_of_values = line.split("\t")
    list_of_values1 = list_of_values.split(" ")
    intermediate.setdefault(key, [])       
    intermediate[key].append(list_of_values1)
for key in intermediate:
    values = intermediate[key]
    for value in values:
        if value[0] == "users":
            for rate in values:
                if rate[0] != "users":
                    result.append((key,value[1],value[2],value[3],value[4],rate[1],rate[2],rate[3]))
                   
for item in result:
    if "none" in item:
        resultj['data'].append({'userid': item[0], 'age': item[1], 'gender': item[2], 'occupation': item[3], 'zipcode': item[4], 'movieid': item[5], 'rating': item[6], 'timestamp': item[7]})
        print (item)   
            
with open('/home/hduser/output_reduce.json', 'w') as outfile:
    try:
        json.dump(resultj, outfile) 
    except ValueError:
        json.dump(resultj, outfile)