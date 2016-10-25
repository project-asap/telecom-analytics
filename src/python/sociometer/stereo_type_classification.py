#
# Copyright 2015-2016 WIND,FORTH
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

"""Stereo Type Classification  Module.

Given a set of user profiles for specific region and weeks of year and a set
of labeled calling behaviors, it returns the percentage of each label
on each spatial region.
E.g.:
Region 1, resident, 75%
Region 1, commuter, 20%

Usage:
    $SPARK_HOME/bin/spark-submit sociometer/stereo_type_classification.py <profiles> <centroids>

Args:
    profiles: The user profiles location. The expected location is expected to match
              the following pattern:
              /profiles/<region>-<start_week>-<end_week> where start_week and
              end_week have the following format: <ISO_year>_<ISO_week>
    centroids: The calling behavior dataset location. The expected location is expected to match
               the following pattern:
               /centroids/<region>-<start_week>-<end_week> where start_week and
               end_week have the following format: <ISO_year>_<ISO_week>

Example:
    $SPARK_HOME/bin/spark-submit sociometer/stereo_type_classification.py /profiles/aree_roma-2015_53-2016_3 \
/centroids/aree_roma-2015_53-2016_3

Results are stored into a local file: sociometer-<region>-<start_week>-<end_week>.

"""

from pyspark import SparkContext
from pyspark.mllib.clustering import KMeansModel

import re
import sys


def user_type(profile, model, centroids):
    if len([x for x in profile if x != 0]) == 1 and sum(profile) < 0.5:
        return 'passing by'
    else:
        idx = model.predict(profile)
        cluster = model.clusterCenters[idx]
        return centroids[cluster]

if __name__ == '__main__':
    sc = SparkContext()
    # annotazione utenti

    d1 = sys.argv[1]
    d2 = sys.argv[1]

    pattern = r'/profiles/(?P<region>\w+)-(?P<start_week>\w+)-(?P<end_week>\w+)'
    m = re.search(pattern, d1)
    region, start_week, end_week = m.groups()

    pattern = r'/centroids/(?P<region>\w+)-(?P<start_week>\w+)-(?P<end_week>\w+)'
    m = re.search(pattern, d2)
    assert((region, start_week, end_week) == m.groups())

    r = sc.pickleFile(d2)
    centroids = {tuple(v.tolist()): k for k, v in r.collect()}
    model = KMeansModel(centroids.keys())

    r = sc.pickleFile(d1)

    # format: (user_id, profile)
    r_auto = r.map(lambda (region, user_id, profile):
                       (region, user_type(profile, model, centroids), user_id, profile)) \
        .map(lambda x: ((x[0], x[1]), 1)) \
        .reduceByKey(lambda x, y: x + y)

    # ottengo coppie municipio,id_cluster

    # risultato finale
    #
    lst = r_auto.collect()
    sociometer = [(x[0], x[1] * 1.0 / sum([y[1]
                                        for y in lst if y[0][0] == x[0][0]])) for x in lst]
    with open("sociometer-%s-%s-%s" %
              (region, start_week, end_week), 'w') as outfile:
        print >>outfile, "region, profile, percentage"
        for s in sorted(sociometer, key=lambda x: x[0][0]):
            print>>outfile, s[0][0], s[0][1].replace("\n", ""), s[1]
