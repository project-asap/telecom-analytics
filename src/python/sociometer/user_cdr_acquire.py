import datetime
from pyspark import SparkContext,StorageLevel,RDD
from pyspark.serializers import MarshalSerializer
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
from sklearn.cluster import KMeans
import numpy as np
import hdfs
import time
import os,sys 
from cdr import CDR,Dataset
from collections import defaultdict




########################functions##################################
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)



def normalize(profilo):
##normalizza giorni chiamate su week end e  workday

	return [(x[0],x[1],x[2],x[3],x[4]*1.0/(2 if x[2]==1 else 5)) for x in profilo]

def check_complete_weeks_fast():
        """ week check with file names. 
        """
        weeks=defaultdict(int)
        for x in hdfs.ls("/"+"ttmetro/roma"):
            for f in hdfs.ls(x):
                try:
                    d=datetime.datetime.strptime(f.split("_")[6].split(".")[0], "%Y%m%d")
                    weeks[(d.isocalendar()[0],d.isocalendar()[1])]+=1
                except:
                    pass
        return [x for x in weeks if weeks[x]>=5]
##########################################################################################


###data loading
#checking file existance
#####
sc=SparkContext()
quiet_logs(sc)



start=time.time()


files=[]

field2col={'user_id':1,'cdr_type':0,'start_cell':9,'end_cell':8,'date':3,'time':4}


weeks=[(2015, 11), (2015, 12), (2015, 13), (2015, 24), (2015, 25), (2015, 26), (2015, 32), (2015, 33), (2015, 34), (2015, 35), (2015, 36), (2015, 37), (2015, 38), (2015, 39), (2015, 41), (2015, 42), (2015, 45), (2015, 46), (2015, 47), (2015, 48), (2015, 49), (2015, 50), (2015, 51), (2015, 52), (2016, 1), (2016, 2), (2016, 3), (2016, 4), (2016, 5), (2016, 6), (2016, 7), (2016, 8), (2016, 9), (2016, 11), (2016, 12), (2016, 13), (2016, 14), (2016, 15), (2016, 16), (2016, 17), (2016, 18), (2016, 19), (2016, 20), (2016, 21), (2016, 22), (2016, 24), (2016, 25)]
#weeks=sorted(check_complete_weeks_fast())

for w in weeks[:]:
    	for x in hdfs.ls("/ttmetro/roma/%s_%s"%(w[0],w[1])):
    	    files.append(x)
r=sc.textFile(','.join(files)).map(lambda x:((x.split(";")[1]),([x.split(";")[21]]))).reduceByKey(lambda x,y: x+y).map(lambda x: (x[0],set(x[1])))

print r.take(5)

print r.map(lambda x: (len(x[1]),1)).reduceByKey(lambda x,y:x+y).collect()
