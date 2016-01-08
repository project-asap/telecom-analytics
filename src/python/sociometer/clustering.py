import datetime
from pyspark import SparkContext, StorageLevel, RDD
from pyspark.serializers import MarshalSerializer
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
from sklearn.cluster import KMeans
import numpy as np

import time
import os, sys

"""
User Profiles clustering module.
Input:
:param 1: region (e.g. 'roma')
:param 2: period (e.g. '06-2015')

Output:
labeled centroids (e.g.: Resident:[....])

"""


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
      
        week_ordering = f7([x[0] for x in obs])
        
        
        carr = [0 for x in range(18)]

        for o in obs:
            week_idx = week_ordering.index(o[0])

            idx = (week_idx) * 6 + o[1] * 3 + o[2]
            carr[idx] = o[3]
        yield carr


def euclidean(v1, v2):
    return sum([abs(v1[i] - v2[i]) ** 2 for i in range(len(v1))]) ** 0.5


region = sys.argv[1]

timeframe = sys.argv[2]

archetipi=sys.argv[3]

k=sys.argv[4]

perc=sys.argv[5]
file=open(archetipi)
archetipi = [(y[1], y[2:][:18]) for y in [x.split(';') for x in file.readlines()]]


# import rdd with profiles

sc = SparkContext()
r = sc.pickleFile('hdfs://hdp1.itc.unipi.it:9000/profiles/' + "%s-%s" % (region, timeframe))

#clustering!

r_carrelli = r.flatMap(lambda x: array_carretto(x[1]))


percentage = int(r_carrelli.count() * float(perc))

data = [np.array(x) for x in r_carrelli.take(percentage)]



kmns = KMeans(n_clusters=int(k), n_init=10, init='random')
kmns.fit(data)
tipi_centroidi = []
centroidi=kmns._cluster_centers
#centroids annotation
for ctr in centroidi:
    tipo_centroide = \
        sorted([(c[0], euclidean(ctr, map(float, c[1]))) for  c in archetipi], key=lambda x: x[1])[0][0]
    tipi_centroidi.append((tipo_centroide, ctr))
os.system("$HADOOP_HOME/bin/hadoop fs -rm -r /profiles/centroids%s-%s"%(region,time))
sc.parallelize(tipi_centroidi).saveAsPickleFile('hdfs://hdp1.itc.unipi.it:9000/profiles/centroids%s-%s'%(region,time))


