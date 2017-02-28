'''
Created on Jun 24, 2016

@author: akhila
'''
from MapReduce import MapReduce
import sys

mr = MapReduce()

def mapper(record):
    print "key is *********",record
    i=0
    while(i<5):
        mr.emit_intermediate((record[0],i),(100,record[1],record[2]))
        
        i+=1
    i=0
    while(i<5):
        mr.emit_intermediate((i,record[1]),(200,record[0],record[2]))
        i+=1
def reducer(key, list_of_values):
    print "list of values is",list_of_values
    a=[]
    b=[]
    for i in list_of_values:
        if i[0]==100:
            print "i[1] is",i[1]
            a.append((i[1],i[2]))
        else:
            b.append((i[1],i[2]))
    product=[]
    for i in a:
        for j in b:
            if i[0]==j[0]:
                print "**************",i[1]
                temp=i[1]*j[1]
                product.append(temp)
    total = 0
    for v in product:
        total += v
    mr.emit((key, total))

# Part 4

inputdata = open(sys.argv[1])

mr.execute(inputdata, mapper, reducer)