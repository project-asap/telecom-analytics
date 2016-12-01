import datetime
from pyspark import SparkContext, StorageLevel, RDD
from pyspark.serializers import MarshalSerializer
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
import numpy as np

import time
import os, sys

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
def array_carretto_rossa(profilo,utente):
    ####trasforma la lista chiamate nel carrellino, con zeri dove non ci sono dati
    def f7(seq):
        seen = set()
        seen_add = seen.add
        return [x for x in seq if not (x in seen or seen_add(x))]

    for munic in set([x[0] for x in profilo ]):
        ##settimana,work/we,timeslice, count normalizzato

        week_ordering = f7([x[1] for x in profilo if x[0] == munic])
        obs = [x[1:] for x in profilo if x[0] == munic]
        obs = sorted(obs, key=lambda d: sum([j[3] for j in obs if j[0] == d[0]]), reverse=True)
      
        week_ordering = f7([x[0] for x in obs ])
        
        
        carr = np.zeros(shape=24)

        for o in obs:
            week_idx = week_ordering.index(o[0])
            if week_idx>3:
                continue
            idx = (week_idx) * 6 + o[1] * 3 + o[2]

            carr[idx] = o[3]
        yield utente,carr,munic
def array_carretto(profilo,user_id):
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


        carr = [0 for x in range(18)]

        for o in obs:

            week_idx = week_ordering.index(o[0])
            if week_idx>3:
                continue
            idx = (week_idx-1) * 6 + o[1] * 3 + o[2]

            carr[idx] = o[3]
        yield munic,carr,user_id
    
region = sys.argv[1]
timeframe = sys.argv[2]

# import rdd with profiles

sc = SparkContext()
quiet_logs(sc)
sc._conf.set('spark.executor.memory','32g').set('spark.driver.memory','32g').set('spark.driver.maxResultsSize','0')
r = sc.pickleFile('hdfs://hdp1.itc.unipi.it:9000/profiles/' + "%s-%s" % (region, timeframe))

k=12
safe_treshold=10

##select alle the users with the same profile as at least other #safe_treshold# users

carrelli = r.flatMap(lambda x: array_carretto(x[1],x[0])).map(lambda x: ((x[0],tuple(x[1][:k])),(x[2],x[1]))).groupByKey() \
           .filter(lambda x: len(list(x[1]))>=safe_treshold).map(lambda x:list(x[1])[1]) #alla fine ho liste di carrelli

print carrelli.first()
