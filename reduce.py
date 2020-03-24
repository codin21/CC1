#!/usr/bin/python3.5
from operator import itemgetter
import sys
import re
import json
a=None
b=0
c=None
d=0
e=None
f=0
g=None
h=0

def evaluate(s):
   i=str(s)+""+op+""+value
   return eval(i)
with open("/home/hduser/outputgroupby.json", "w") as outfile:
    dlist=[]

    
    for line in sys.stdin:
        line = line.strip()
        groupby,funcarg,func,op,value,an,col1=re.split(r'[_\s]\s*',line)
        agg=func+"("+an+")"
        dict={}	
        try:
            funcarg = int(funcarg)
        except ValueError:
            continue
        if func=="sum":
    
           if a == groupby:
        
                b = funcarg + b
           else:
                if (a and evaluate(b)):
            # write result to STDOUT
			
                    dict.update( {col1 : a,agg:b} )
                    dlist.append(dict)
                
                    #json.dump(a, outfile)
                    #json.dump(b, outfile)
                    print ('%s\t%s' % (a, b))
                    b=0
                b = funcarg + b
                a = groupby
        elif func=="count":
    
            if c == groupby:
        
                d = d + 1
            else:
                if (c and evaluate(d)):
				    
                    dict.update( {col1 : c,agg:d} )
                    dlist.append(dict)
            # write result to STDOUT
               
                    #json.dump(c, outfile)
                    #json.dump(d, outfile)
                    print ('%s\t%s' % (c, d))
                    d=0
                d = d + 1
                c = groupby
        elif func=="max":
            if e == groupby:
                if funcarg > f:
                    f = funcarg
            else:
                if (e and evaluate(f)):
            # write result to STDOUT
                  
                    dict.update( {col1 : e,agg:f} )
                    dlist.append(dict)  
                    #json.dump(e, outfile)
                    #json.dump(f, outfile)
                    print ('%s\t%s' % (e, f))
                f = funcarg
                e = groupby
        elif func=="min":
            if g == groupby:
                if funcarg < f:
                    h = funcarg
            else:
                if (g and evaluate(h)):
            # write result to STDOUT
                    dict.update( {col1 : g,agg:h} )
                    dlist.append(dict)
        
                    #json.dump(g, outfile)
                    #json.dump(h, outfile)
                    print ('%s\t%s' % (g, h))
                h = funcarg
                g = groupby	
    dict={}	
    if ((a == groupby) and evaluate(b)):
    
        dict.update( {col1 : a,agg:b} )
        dlist.append(dict)
        #json.dump(a, outfile)
        #json.dump(b, outfile)
        print ('%s\t%s' % (a, b))
    elif ((c == groupby) and evaluate(d)):
        dict.update( {col1 : c,agg:d} )
        dlist.append(dict)
        
        #json.dump(c, outfile)
        #json.dump(d, outfile)
        print ('%s\t%s' % (c, d))
    elif ((e == groupby) and evaluate(f)):
        dict.update( {col1 : e,agg:f} )
        dlist.append(dict)
        #json.dump(e, outfile)
        #json.dump(f, outfile)
        print ('%s\t%s' % (e, f))
    elif ((g == groupby) and evaluate(h)):
        dict.update( {col1 : g,agg:h} )
        dlist.append(dict)
        #json.dump(g, outfile)
        #json.dump(h, outfile)
		#outfile.close()
        print ('%s\t%s' % (g, h))
    json.dump(dlist,outfile)
