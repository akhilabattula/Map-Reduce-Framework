'''
Created on Jun 23, 2016

@author: akhila
'''


import re

import sys

from MapReduce import MapReduce


mr = MapReduce()

def mapper(record):
    key = record[0]
    #print "key is",key
    value = record[1]
    #words = value.split()
    words1= re.findall(r'[\w]+',value)
    #print "words here",words1
    for w in words1:
        mr.emit_intermediate(w.lower(),(key,1))
        
def reducer(key, list_of_values):
    total = 0
   
    #globaldicthere={}
    #print "list of values is",key,list_of_values[0]
    globaldicthere=combiner(list_of_values)
    #print "len is",globaldicthere
    for i in globaldicthere:
        for v in globaldicthere[i]:
            total=total+v
        globaldicthere[i]=[]
        globaldicthere[i].append(total)
        total=0
    #print "globaldict here is",globaldicthere   
    #print "len of globaldict",len(globaldicthere)
    
    mr.emit((key,len(globaldicthere),globaldicthere))

# Part 4
def combiner(listval):
    #print "received list val is",listval
    globaldict={}
    for i in listval:
        if i[0] in globaldict:
            #print "went into if"
            globaldict[i[0]].append(i[1])
            
        else:
            #print "went into else"
            globaldict.setdefault(i[0], [])
            globaldict[i[0]].append(i[1])
            
    return globaldict

inputdata = open(sys.argv[1])

mr.execute(inputdata, mapper, reducer)
