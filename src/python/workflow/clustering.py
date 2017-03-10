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

import datetime
import os
import sys

from pyspark import SparkContext
from pyspark.mllib.clustering import KMeans

from dateutil import rrule
from utils import quiet_logs

"""Profiles Clustering modules Module

Given a set of user profiles, it returns typical calling behaviors (clusters)
and a label for each behavior (i.e. resident, commuter, etc.)
More specifically, the clustering algorithm used is mllib KMeans and
the cluster labels are computed by the minimum euclidean distance of the cluster center
and a number of labeled characteristic behaviors.

Usage:
    $SPARK_HOME/bin/spark-submit sociometer/clustering.py <profiles dataset> <region> <start_date> <end_date>

Args:
    profile dataset: The profiles dataset prefix. Can be any Hadoop-supported file system URI.
                     The full path dataset name it computed as:
                     <profile dataset>/<region>/<start_date>_end_date>
                     The expected dataset schema is:
                     <region>,<user_id>,<profile>.
                     The <profile> is a 24 element list containing the sum of user calls for each time division.
                     The column index for each division is: <week_idx> * 6 + <is_weekend> * 3 + <timeslot>
                     where <is_weekend> can be 0 or 1 and <timeslot> can be 0, 1, or 2.
    tag: The tag name featuring in the stored results
    start_date: The analysis starting date. Expected input %Y-%m-%d
    end_date: The analysis ending date. Expected input %Y-%m-%d

Results are stored into several hdfs files: /centroids/<region>/<year>_<week_of_year>
where <year> and <week_of_year> are the year and week of year index of the starting week
of the 4 week analysis.

Example:
    $SPARK_HOME/bin/spark-submit \
        sociometer/user_profiling.py hdfs:///profiles/roma \
        roma 2016-01-01 2016-01-31

The results will be sotred in the hdfs files:
/centroids/roma/2015_53
/centroids/roma/2016_01
/centroids/roma/2016_02 etc
"""


def euclidean(v1, v2):
    return sum([abs(v1[i] - v2[i]) ** 2 for i in range(len(v1))]) ** 0.5


ARG_DATE_FORMAT = '%Y-%m-%d'

folder = sys.argv[1]
tag = sys.argv[2]
start_date = datetime.datetime.strptime(sys.argv[3], ARG_DATE_FORMAT)
end_date = datetime.datetime.strptime(sys.argv[4], ARG_DATE_FORMAT)

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

archetipi = [(y[1], y[2:]) for y in [x.split(';')
                                     for x in archetipi.split("\n")[:-1]]]

weeks = [d.isocalendar()[:2] for d in rrule.rrule(
    rrule.WEEKLY, dtstart=start_date, until=end_date
)]

sc = SparkContext()
quiet_logs(sc)
# sc._conf.set('spark.executor.memory','32g').set('spark.driver.memory','32g').set('spark.driver.maxResultsSize','0')
for year, week in weeks:
    subfolder = "%s/%s/%s_%s" % (folder, tag, year, week)
    exists = os.system("$HADOOP_PREFIX/bin/hdfs dfs -test -e %s" % subfolder)
    if exists != 0:
        continue
    r = sc.textFile(subfolder)
    if r.isEmpty():
        continue

    # clustering!
    clusters = KMeans.train(r.map(lambda x: eval(x)[2]), 100, maxIterations=20,
                            runs=5, initializationMode="random")

    tipi_centroidi = []
    # centroids annotation
    centroidi = clusters.centers
    for ctr in centroidi:
        tipo_centroide = \
            sorted([(c[0], euclidean(ctr, map(float, c[1])))
                    for c in archetipi], key=lambda x: x[1])[0][0]
        tipi_centroidi.append((tipo_centroide, list(ctr)))

    output = "/centroids/%s/%s_%s" % (tag, year, week)
    os.system("$HADOOP_HOME/bin/hadoop fs -rm -r %s" % output)
    sc.parallelize(tipi_centroidi).saveAsTextFile(output)
