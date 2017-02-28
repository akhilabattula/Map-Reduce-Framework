'''
Created on Jun 24, 2016

@author: akhila
'''
from MapReduce import MapReduce
import sys


mr = MapReduce()

def mapper(record):
    #print "record is",record
    for i in range(0,len(record)):
        for j in range(i+1,len(record)):
            mr.emit_intermediate((record[i],record[j]), 1)
        
def reducer(key, list_of_values):
    total = 0
    for v in list_of_values:
        total += v
    if (total>100):
        mr.emit(key)

# Part 4

inputdata = open(sys.argv[1])

mr.execute(inputdata, mapper, reducer)
