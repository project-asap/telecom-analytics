import datetime
from pyspark import SparkContext,StorageLevel,RDD
from pyspark.serializers import MarshalSerializer
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
from collections import defaultdict
import hdfs
import cdr

import numpy as np


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
from itertools import groupby as g
def most_common_oneliner(L):
    return max(g(sorted(L)), key=lambda(x, v):(len(list(v)),-L.index(x)))[0]
    
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



sc=SparkContext("spark://131.114.136.218:7077")
quiet_logs(sc)
##annotazione utenti

##open
folder,region=sys.argv[1],sys.argv[2]

#import cPickle as pk
#weeks_dict=pk.load(open("weeks_dict.pkl","rb"))
weeks_dict=cdr.check_complete_weeks_fast(folder)
weeks=sorted(weeks_dict.keys())
print weeks
ann_file=[]
for timeframe in weeks:
    print timeframe
    try:
        len(hdfs.ls('/profiles/'+"%s-%s_%s"%(region,timeframe[0],timeframe[1])))
        len(hdfs.ls('/profiles/'+"centroids-%s-%s_%s"%(region,timeframe[0],timeframe[1])))
    except ValueError:
        continue
    r=sc.pickleFile('hdfs://hdp1.itc.unipi.it:9000/profiles/centroids-%s-%s_%s'%(region,timeframe[0],timeframe[1]))
    cntr=r.collect()

    profiles=[(x[0],x[1]) for x in cntr]
    r=sc.pickleFile('hdfs://hdp1.itc.unipi.it:9000/profiles/'+"%s-%s_%s"%(region,timeframe[0],timeframe[1]))
    #r_id= r.flatMap(lambda x:  annota_utente(x[1],profiles,x[0])).collect()
    #format: (profilo,area,count)
    r_auto= r.flatMap(lambda x:  annota_utente(x[1],profiles,x[0])) \
   .map(lambda x: ((x[0],x[1]),1)) \
   .reduceByKey(lambda x,y:x+y)

    ###save for Gabrielli
    r_gabrielli= r.flatMap(lambda x:  annota_utente(x[1],profiles,x[0]))

    os.system("$HADOOP_HOME/bin/hadoop fs -rm -r /annotation/%s-%s_%s"%(region,timeframe[0],timeframe[1]))
    #print r_gabrielli.map(lambda x: (x[2],x[0]),x[1]).groupByKey().first()
    r_gabrielli.map(lambda x: "%s;%s;%s"%(x[2],x[0],x[1])).saveAsTextFile("hdfs://hdp1.itc.unipi.it:9000/annotation/%s-%s_%s"%(region,timeframe[0],timeframe[1]))
    ann_file.append("hdfs://hdp1.itc.unipi.it:9000/annotation/%s-%s_%s"%(region,timeframe[0],timeframe[1]))
##ottengo coppie municipio,id_cluster

### risultato finale
#
    lst=r_auto.collect()
    sociometer=[(x[0],x[1]*1.0/sum([y[1] for y in lst if y[0][0]==x[0][0]])) for x in lst]
    outfile=open("results/sociometer-%s-%s_%s"%(region,timeframe[0],timeframe[1]),'w')
    #print >>outfile,"municipio, profilo, percentage"
    for s in sorted(sociometer,key=lambda x: x[0][0]):
        print>>outfile, s[0][0],s[0][1].replace("\n",""),s[1]

annotation=sc.textFile(",".join(ann_file)).map(lambda x: ((x.split(";")[0],x.split(";")[1]),x.split(";")[2])).groupByKey()
os.system("$HADOOP_HOME/bin/hadoop fs -rm -r /annotation_global/%s"%region)
annotation.map(lambda x: "%s;%s;%s"%(x[0][0],x[0][1],most_common_oneliner(list(x[1])))).saveAsTextFile("hdfs://hdp1.itc.unipi.it:9000/annotation_global/%s"%region)
for f in ann_file:
    os.system("$HADOOP_HOME/bin/hadoop fs -rm -r /annotation/%s-%s_%s"%(region,timeframe[0],timeframe[1]))