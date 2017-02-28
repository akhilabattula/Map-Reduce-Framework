import json

class MapReduce:
    def __init__(self):
        # initialize dictionary for intermediate values from Map task
        self.intermediate = {}
        # initialize list for results of Reduce task
        self.result = []

    def emit_intermediate(self, key, value):
        print "received values are",key,value
        # if key not already in dictionary, set value to empty list
        self.intermediate.setdefault(key, [])
        # add value to list associated with key
        self.intermediate[key].append(value)
        #print "now intermediate is",self.intermediate

    def emit(self, value):
        # append value to list of results
        self.result.append(value) 

    def execute(self, data, mapper, reducer):
        # read each line from input file; call Map function on each record
        for line in data:
            #print "line is",line
            record = json.loads(line)
            #print "record is",record
            mapper(record)
        print "***********"
        
        # for each key:valuelist in intermediate dictionary, call Reduce task
        print "calling reducer",self.intermediate
        for key in self.intermediate:
            #print "calling reducer for",key
            reducer(key, self.intermediate[key])

        jenc = json.JSONEncoder()
        # print each result in result list
        for item in self.result:
            #print "went here",type(item)
            print jenc.encode(item)
