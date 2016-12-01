import datetime
from pyspark import SparkContext,StorageLevel,RDD
from pyspark.serializers import MarshalSerializer
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
from collections import defaultdict
import hdfs

import numpy as np
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

"""
Stereo Type Classification  Module

Given a set of users' profiles and a set of labeled calling behaviors, it returns the percentage of each label
on each spatial region. 
E.g.: 
Region 1, resident, 75%
Region 1, commuter, 20%
...


Usage: stereo_type_classification.py  <region> <timeframe> 

--region,timeframe: file name desired for the stored results. E.g. Roma 11-2015

example: pyspark stereo_type_classification.py  roma 06-2015

Results are stored into file: sociometer-<region>-<timeframe>.csv

"""


import os,sys

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
def euclidean(v1,v2):

    return sum([(v1[i]-v2[i])**2 for i in range(len(v1))])**0.5

def annota_utente(profilo,profiles,id_utente):
    def f7(seq):
        seen = set()
        seen_add = seen.add
        return [x for x in seq if not (x in seen or seen_add(x))]
    for munic in set([x[0] for x in profilo]):
        ##settimana,work/we,timeslice, count normalizzato
        week_ordering=f7([x[1] for x in profilo if x[0]==munic])
        
        obs=[x[1:] for x in profilo if x[0]==munic]
        #carr=np.zeros(24)
        carr=[0 for x in range(24)]
        for o in obs:
            week_idx=week_ordering.index(o[0])
            if week_idx>3:
                continue
            idx=(week_idx-1)*6+o[1]*3+o[2]
            carr[idx]=o[3]
        tipo_utente=sorted([(c[0],euclidean(carr,list(c[1]))) for c in profiles],key=lambda x:x[1])[0][0]
        if len([x for x in carr if x!=0])==1 and sum(carr)<0.5:
            tipo_utente='passing by'
        yield (munic,tipo_utente,id_utente,carr)



sc=SparkContext()
quiet_logs(sc)
##annotazione utenti

##open
region,timeframe='roma','2016_9'

#weeks=[(2015, 11), (2015, 12), (2015, 13), (2015, 24), (2015, 25), (2015, 26), (2015, 32), (2015, 33), (2015, 34), (2015, 35), (2015, 36), (2015, 37), (2015, 38), (2015, 39), (2015, 41), (2015, 42), (2015, 45), (2015, 46), (2015, 47), (2015, 48), (2015, 49), (2015, 50), (2015, 51), (2015, 52), (2016, 1), (2016, 2), (2016, 3), (2016, 4), (2016, 5), (2016, 6), (2016, 7), (2016, 8), (2016, 9), (2016, 11), (2016, 12), (2016, 13), (2016, 14), (2016, 15), (2016, 16), (2016, 17), (2016, 18), (2016, 19), (2016, 20), (2016, 21), (2016, 22), (2016, 24), (2016, 25)]
#weeks=[(2015, 34), (2015, 35), (2015, 36), (2015, 37), (2015, 38), (2015, 39), (2015, 41), (2015, 42), (2015, 45), (2015, 46), (2015, 47), (2015, 48), (2015, 49), (2015, 50), (2015, 51), (2015, 52), (2016, 1), (2016, 2), (2016, 3), (2016, 4), (2016, 5), (2016, 6), (2016, 7), (2016, 8), (2016, 9), (2016, 11), (2016, 12), (2016, 13), (2016, 14), (2016, 15), (2016, 16), (2016, 17), (2016, 18), (2016, 19), (2016, 20), (2016, 21), (2016, 22), (2016, 24), (2016, 25)]
import cPickle as pk
weeks_dict=pk.load(open("weeks_dict.pkl","rb"))

weeks=sorted(weeks_dict.keys())
for timeframe in weeks:
    print timeframe
    try:
        len(hdfs.ls('/profiles/'+"%s-%s_%s"%(region,timeframe[0],timeframe[1])))
    except ValueError:
        continue
    r=sc.pickleFile('hdfs://hdp1.itc.unipi.it:9000/profiles/centroids-%s-%s_%s'%(region,timeframe[0],timeframe[1]))
    cntr=r.collect()

    profiles=[(x[0],x[1]) for x in cntr]
    r=sc.pickleFile('hdfs://hdp1.itc.unipi.it:9000/profiles/'+"%s-%s_%s"%(region,timeframe[0],timeframe[1]))
    #r_id= r.flatMap(lambda x:  annota_utente(x[1],profiles,x[0])).collect()

    #format: (id_utente,profilo)
    r_auto= r.flatMap(lambda x:  annota_utente(x[1],profiles,x[0])) \
   .map(lambda x: ((x[0],x[1]),1)) \
   .reduceByKey(lambda x,y:x+y)

    ###save for Gabrielli
    r_gabrielli= r.flatMap(lambda x:  annota_utente(x[1],profiles,x[0]))

    os.system("$HADOOP_HOME/bin/hadoop fs -rm -r /annotation/%s-%s_%s"%(region,timeframe[0],timeframe[1]))
    r_gabrielli.saveAsTextFile("hdfs://hdp1.itc.unipi.it:9000/annotation/%s-%s_%s"%(region,timeframe[0],timeframe[1]))

##ottengo coppie municipio,id_cluster

### risultato finale
#
    lst=r_auto.collect()
    sociometer=[(x[0],x[1]*1.0/sum([y[1] for y in lst if y[0][0]==x[0][0]])) for x in lst]
    outfile=open("results/sociometer-%s-%s_%s"%(region,timeframe[0],timeframe[1]),'w')
    print >>outfile,"municipio, profilo, percentage"
    for s in sorted(sociometer,key=lambda x: x[0][0]):
        print>>outfile, s[0][0],s[0][1].replace("\n",""),s[1]


