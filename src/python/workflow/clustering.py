import datetime
from pyspark import SparkContext, StorageLevel, RDD
from pyspark.serializers import MarshalSerializer
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
import numpy as np
import hdfs
import time
import os, sys
import cdr
"""
Profiles Clustering modules Module

Given a set of users' profiles, it returns typical calling behaviors (clusters) 
and a label for each behavior (i.e. resident, commuter, etc.)

Usage: clustering.py  <region> <timeframe> 

--region,timeframe: file name desired for the stored results. E.g. Roma 11-2015

example: pyspark clustering.py roma 06-2015

Results are stored into hdfs: /centroids-<region>-<timeframe>

"""

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def array_carretto(profilo):
    ####trasforma la lista chiamate nel carrellino, con zeri dove non ci sono dati
    def f7(seq):
        seen = set()
        seen_add = seen.add
        return [x for x in seq if not (x in seen or seen_add(x))]

    for munic in set([x[0] for x in profilo]):
        ##settimana,work/we,timeslice, count normalizzato

        week_ordering = f7([x[1] for x in profilo if x[0] == munic])
        obs = [x[1:] for x in profilo if x[0] == munic]
        obs = sorted(obs, key=lambda d: sum([j[3] for j in obs if j[0] == d[0]]), reverse=True)

        week_ordering = f7([x[0] for x in obs ])


        carr = [0 for x in range(24)]

        for o in obs:

            week_idx = week_ordering.index(o[0])
            if week_idx>4:
                continue
            idx = (week_idx) * 6 + o[1] * 3 + o[2]

            carr[idx] = o[3]
        yield carr,munic,profilo

def euclidean(v1, v2):
    return sum([abs(v1[i] - v2[i]) ** 2 for i in range(len(v1))]) ** 0.5


folder,region=sys.argv[1],sys.argv[2]



archetipi="""0;resident;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0
1;resident;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5;0.5
2;resident; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1
3;dynamic_resident;0.0;0.0;0.0;1.0;1.0;1.0;0.0;0.0;0.0;1.0;1.0;1.0;0.0;0.0;0.0;1.0;1.0;1.0;0.0;0.0;0.0;1.0;1.0;1.0
4;dynamic_resident;0.0;0.0;0.0;0.5;0.5;0.5;0.0;0.0;0.0;0.5;0.5;0.5;0.0;0.0;0.0;0.5;0.5;0.5;0.0;0.0;0.0;0.5;0.5;0.5
5;dynamic_resident;0.0;0.0;0.0; 0.1; 0.1; 0.1;0.0;0.0;0.0; 0.1; 0.1; 0.1;0.0;0.0;0.0; 0.1; 0.1; 0.1;0.0;0.0;0.0; 0.1; 0.1; 0.1
5;commuter;1.0;1.0;1.0;0.0;0.0;0.0;1.0;1.0;1.0;0.0;0.0;0.0;1.0;1.0;1.0;0.0;0.0;0.0;1.0;1.0;1.0;0.0;0.0;0.0
6;commuter;0.5;0.5;0.5;0.0;0.0;0.0;0.5;0.5;0.5;0.0;0.0;0.0;0.5;0.5;0.5;0.0;0.0;0.0;0.5;0.5;0.5;0.0;0.0;0.0
7;commuter; 0.1; 0.1; 0.1;0.0;0.0;0.0; 0.1; 0.1; 0.1;0.0;0.0;0.0; 0.1; 0.1; 0.1;0.0;0.0;0.0; 0.1; 0.1; 0.1;0.0;0.0;0.0
8;visitor;1.0;1.0;1.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0
9;visitor;0.5;0.5;0.5;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0
10;visitor; 0.1; 0.1; 0.1;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0
11;resident;1.0;1.0;1.0;1.0;1.0;1.0;0.5;0.5;0.5;0.5;0.5;0.5; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1;0.0;0.0;0.0;0.0;0.0;0.0
12;resident;0.5;0.5;0.5;0.5;0.5;0.5; 0.1; 0.1; 0.1; 0.1; 0.1; 0.1;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0
13;visitor;0.0;0.0;0.0;1.0;1.0;1.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0
14;visitor;0.0;0.0;0.0;0.5;0.5;0.5;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0
15;visitor;0.0;0.0;0.0; 0.1; 0.1; 0.1;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0;0.0"""

archetipi = [(y[1], y[2:]) for y in [x.split(';') for x in archetipi.split("\n")[:-1]]]


# import rdd with profiles
#import cPickle as pk
#weeks_dict=pk.load(open("weeks_dict.pkl","rb"))
weeks_dict=cdr.check_complete_weeks_fast(folder)

sc=SparkContext("spark://131.114.136.218:7077")
quiet_logs(sc)
sc._conf.set('spark.executor.memory','32g').set('spark.driver.memory','32g').set('spark.driver.maxResultsSize','0')
for timeframe in sorted(weeks_dict.keys())[3:]:
    try:
        len(hdfs.ls('/profiles/'+"%s-%s_%s"%(region,timeframe[0],timeframe[1])))
    except ValueError:
        continue
    
    r = sc.pickleFile('hdfs://hdp1.itc.unipi.it:9000/profiles/' + "%s-%s_%s" % (region, timeframe[0],timeframe[1]))
    #clustering!
    print region, timeframe

    r_carrelli = r.flatMap(lambda x: array_carretto(x[1]))

    r_check=r_carrelli.map(lambda x: [sum(x[0][:6]),sum(x[0][6:12]),sum(x[0][12:18]),sum(x[0][-6:])])
    #print r_check.filter(lambda x: x==sorted(x,reverse=True)).count(),r_check.filter(lambda x: x!=sorted(x,reverse=True)).count()
   

    clusters = KMeans.train(r_carrelli.map(lambda x: x[0]), 100, maxIterations=10,
        runs=1, initializationMode="random")



    tipi_centroidi = []
    #centroids annotation
    centroidi=clusters.centers
    for ctr in centroidi:
        tipo_centroide = sorted([(c[0], euclidean(ctr, map(float, c[1]))) for  c in archetipi], key=lambda x: x[1])[0][0]
        l=[sum(ctr[:6]),sum(ctr[6:12]),sum(ctr[12:18]),sum(ctr[-6:])]
        if sorted(l,reverse=True)!=l:
            print l,ctr,tipo_centroide
        else:
            print "centroid ok"
        tipi_centroidi.append((tipo_centroide, ctr))
    
    os.system("$HADOOP_HOME/bin/hadoop fs -rm -r /profiles/centroids-%s-%s_%s"%(region,timeframe[0],timeframe[1]))
    sc.parallelize(tipi_centroidi).saveAsPickleFile("hdfs://hdp1.itc.unipi.it:9000/profiles/centroids-%s-%s_%s"%(region,timeframe[0],timeframe[1]))


