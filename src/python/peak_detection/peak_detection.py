__author__ = 'paul'
import datetime
from pyspark import SparkContext,StorageLevel,RDD
import hdfs
from pyspark.serializers import MarshalSerializer
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
from sklearn.cluster import KMeans
import numpy as np
import os,sys

"""
Peak detection Module

Given a hourly presence dataset (usually regarding a month of activity), and a typical weekly presence dataset, it computes
the relative presences for each hour of the month, in order to identify eventual peaks of presences.

Usage: peak_detection.py  <spatial_division> <region> <timeframe> 

--region,timeframe: names of the file stored into the hdfs. E.g. Roma 11-2015

example: pyspark peak_detection.py roma 06-215

It loads the hourly presences in /peaks/weekly_presence-<region>-<timeframe> and stores 
results into standard csv file: rome_peaks<region>-<timeframe>-<spatial_division>.csv

"""

spatial_division=sys.argv[1]
region=sys.argv[2]
timeframe=sys.argv[3]

sc=SparkContext()


presenze_medie=sc.pickleFile('hdfs://hdp1.itc.unipi.it:9000/peaks/weekly_presence-'+"%s-%s"%(region,timeframe)).collectAsMap()

chiamate_orarie=sc.pickleFile('hdfs://hdp1.itc.unipi.it:9000/peaks/orarie_presence-'+"%s-%s"%(region,timeframe))


peaks=open('rome_peaks%s-%s-%s.csv'%(region,timeframe,spatial_division.replace(".","").replace("/","")),'w')
for l in  chiamate_orarie.collect():
    print >>peaks, "%s,%s,%s,%s"%(l[0][0],l[0][4],l[0][3],l[1]/np.mean(list(presenze_medie[(l[0][0],l[0][1],l[0][3])])))