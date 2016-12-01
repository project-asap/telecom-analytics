import datetime
from pyspark import SparkContext,StorageLevel,RDD
from pyspark.serializers import MarshalSerializer
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
from collections import defaultdict
import hdfs

import numpy as np


"""
User label extraction Module

Extract labels from annotation files
...


Usage: label_extract.py  <region> <timeframe> 


"""


import os,sys
region='roma'

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)




sc=SparkContext()
quiet_logs(sc)
##annotazione utenti

##open


import cPickle as pk
weeks_dict=pk.load(open("weeks_dict.pkl","rb"))

weeks=sorted(weeks_dict.keys())
files=[]
for timeframe in weeks:
    try :
        hdfs.ls("/annotation/%s-%s_%s"%(region,timeframe[0],timeframe[1]))
        files+=["hdfs://hdp1.itc.unipi.it:9000/annotation/%s-%s_%s"%(region,timeframe[0],timeframe[1])]
    except ValueError:
        pass
        
rdd=sc.textFile(','.join(files)).map(lambda x: x.split(",")).map(lambda x: ((x[0].strip("("), x[2]),x[1] )).groupByKey().map(lambda x: (x[0],list(x[1])))
rdd.saveAsTextFile("hdfs://hdp1.itc.unipi.it:9000/annotation_global-%s"%region)


