from pyspark import SparkContext
from pyspark.mllib.clustering import KMeans
import numpy as np

import os
import sys

"""
Profiles Clustering modules Module

Given a set of users' profiles, it returns typical calling behaviors (clusters)
and a label for each behavior (i.e. resident, commuter, etc.)

Usage: clustering-mllib.py  <region> <timeframe>

--region,timeframe: file name desired for the stored results. E.g. Roma 11-2015

example: pyspark clustering-mllib.py roma 06-2015

Results are stored into hdfs: /centroids-<region>-<timeframe>

"""


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def array_carretto(profilo):
    # trasforma la lista chiamate nel carrellino, con zeri dove non ci sono
    # dati
    def f7(seq):
        seen = set()
        seen_add = seen.add
        return [x for x in seq if not (x in seen or seen_add(x))]

    for munic in set([x[0] for x in profilo]):
        # settimana,work/we,timeslice, count normalizzato

        week_ordering = f7([x[1] for x in profilo if x[0] == munic])
        obs = [x[1:] for x in profilo if x[0] == munic]
        obs = sorted(obs, key=lambda d: sum(
            [j[3] for j in obs if j[0] == d[0]]), reverse=True)

        week_ordering = f7([x[0] for x in obs])

        carr = [0 for x in range(18)]

        for o in obs:

            week_idx = week_ordering.index(o[0])
            if week_idx > 3:
                continue
            idx = (week_idx) * 6 + o[1] * 3 + o[2]

            carr[idx] = o[3]
        yield carr


def euclidean(v1, v2):
    return sum([abs(v1[i] - v2[i]) ** 2 for i in range(len(v1))]) ** 0.5


archetipi = """0;resident;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0;1.0
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

archetipi = [(y[1], y[2:][:18]) for y in [x.split(';')
                                          for x in archetipi.split("\n")[:-1]]]

if __name__ == '__main__':
    region = sys.argv[1]
    timeframe = sys.argv[2]

    # import rdd with profiles

    sc = SparkContext()
    quiet_logs(sc)
    sc._conf.set('spark.executor.memory', '32g').set(
        'spark.driver.memory', '32g').set('spark.driver.maxResultsSize', '0')
    r = sc.pickleFile('/profiles-%s-%s' % (region, timeframe))

    # clustering!

    r_carrelli = r.flatMap(lambda x: array_carretto(x[1]))

    percentage = 0.3
    r_carrelli.sample(False, percentage, 0).filter(
        lambda l: sum(l))  # filtro passing by
    # sample and filter out passing by
    data = r_carrelli.sample(False, percentage, 0).filter(
        lambda l: sum(l)).map(lambda x: np.array(x))

    #kmns = KMeans.train(data, 100, initializationMode="random")
    kmns = KMeans.train(data, 100, initializationMode="k-means||")
    tipi_centroidi = []
    # centroids annotation
    centroidi = kmns.centers

    for ctr in centroidi:
        tipo_centroide = \
            sorted([(c[0], euclidean(ctr, map(float, c[1])))
                    for c in archetipi], key=lambda x: x[1])[0][0]
        tipi_centroidi.append((tipo_centroide, ctr))
    os.system("$HADOOP_HOME/bin/hadoop fs -rm -r /centroids-%s-%s" %
              (region, timeframe))
    sc.parallelize(tipi_centroidi).saveAsPickleFile(
        '/centroids-%s-%s' % (region, timeframe))
