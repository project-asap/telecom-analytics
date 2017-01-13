#
# Copyright 2015-2017 WIND,FORTH
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from pyspark import SparkContext
from pyspark.mllib.clustering import KMeansModel

from cdr import explore_input
from utils import quiet_logs

"""
Stereo Type Classification  Module

Given a set of users' profiles and a set of labeled calling behaviors, it returns the percentage of each label
on each spatial region.
E.g.:
Region 1, resident, 75%
Region 1, commuter, 20%
...


Usage: stereo_type_classification.py  <area> <timeframe>

--area,timeframe: file name desired for the stored results. E.g. Roma 11-2015

example: pyspark stereo_type_classification.py  roma 06-2015

Results are stored into file: sociometer-<area>-<timeframe>.csv

"""


import os,sys
from itertools import groupby as g
def most_common_oneliner(L):
    return max(g(sorted(L)), key=lambda(x, v): (len(list(v)), -L.index(x)))[0]

def user_type(profile, model, centroids):
    if len([x for x in profile if x != 0]) == 1 and sum(profile) < 0.5:
        return 'passing by'
    else:
        idx = model.predict(profile)
        cluster = model.clusterCenters[idx]
        return centroids[cluster]


sc=SparkContext()
quiet_logs(sc)

profiles = sys.argv[1]
centroids = sys.argv[2]
area = sys.argv[3]

_, weeks = explore_input(
    profiles,
    lambda (n, d): True,
    lambda n: tuple(map(int, n.split('/')[-1].split('_')))
)

ann_file=[]
r_gabrielli = sc.emptyRDD()
for year, week in weeks:
    subfolder = "%s/%s_%s" % (centroids, year, week)
    exists = os.system("$HADOOP_PREFIX/bin/hdfs dfs -test -e %s" % subfolder)
    if exists != 0:
        continue
    rdd = sc.textFile(subfolder).map(lambda e: eval(e))
    cntr = {tuple(v): k for k, v in rdd.collect()}
    model = KMeansModel(cntr.keys())

    subfolder = "%s/%s_%s" % (profiles, year, week)
    rdd = sc.textFile(subfolder).map(lambda e: eval(e))
    r_auto = rdd.map(lambda (region, user, profile):
                    (region, user, user_type(profile, model, cntr)))

    r_gabrielli = r_gabrielli.union(r_auto.map(lambda (region, user, user_class):
                                 ((user, region), [user_class])))

    r_auto = r_auto.map(lambda (region, _, user_class): ((region, user_class), 1)) \
        .reduceByKey(lambda x, y: x + y)
    lst = r_auto.collect()
    sociometer=[(region,
                 user_class,
                 count * 1.0 / sum([c for ((r, uc), c) in lst if r == region]))
                for ((region, user_class), count) in lst]
    outfile=open("sociometer-%s-%s_%s"%(area, year, week),'w')
    for region, uclass, count in sorted(sociometer, key = lambda x: x[0]):
        print>>outfile, region, uclass, count

os.system("$HADOOP_HOME/bin/hadoop fs -rm -r /annotation_global/%s" % area)
r_gabrielli.reduceByKey(lambda x, y: x + y) \
                .map(lambda ((user, region), l): (user, region, most_common_oneliner(l))) \
                .saveAsTextFile('/annotation_global/%s' % area)
