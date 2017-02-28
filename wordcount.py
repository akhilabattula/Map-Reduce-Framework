'''
Created on Jun 23, 2016

@author: akhila
'''
from MapReduce import MapReduce
import sys

mr = MapReduce()

def mapper(record):
    key = record[0]
    print "key is *********",key
    value = record[1]
    words = value.split()
    for w in words:
        mr.emit_intermediate(w, 1)
        
def reducer(key, list_of_values):
    total = 0
    for v in list_of_values:
        total += v
    mr.emit((key, total))

# Part 4

inputdata = open(sys.argv[1])

mr.execute(inputdata, mapper, reducer)