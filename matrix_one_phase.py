'''
Created on Jun 24, 2016

@author: akhila
'''
from MapReduce import MapReduce
import sys
import json

mr = MapReduce()

def mapper(r):
    #print "record is",record
    
        mr.emit_intermediate(r[1],(1,r[0],r[2]))
        mr.emit_intermediate(r[0],(2,r[1],r[2]))
    
def reducer(key, list_of_values):
    #print "list of values is",list_of_values
    a=[]
    b=[]
    globaldict={}
    for u in list_of_values:
        if u[0]==1:
            a.append((u[1],u[2]))
        else:
            b.append((u[1],u[2]))
    for i in a:
        for k in b:
            globaldict[(i[0],k[0])]=(i[1]*k[1])
    #print "a is",a
   
    
    #print "b is",b
    print "globaldict is",globaldict
    for i in globaldict:
        print i[0],i[1] 
        mr.emit((i[0],i[1],globaldict[i]))
        json.dump((i[0],i[1],globaldict[i]),fp )
             
           


# Part 4
fp=open('output.json', 'wb+')
inputdata = open(sys.argv[1])

mr.execute(inputdata, mapper, reducer)


    