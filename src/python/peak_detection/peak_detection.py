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
__author__ = 'paul'
from pyspark import SparkContext
import numpy as np
import sys

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

spatial_division = sys.argv[1]
region = sys.argv[2]
timeframe = sys.argv[3]

sc = SparkContext()


presenze_medie = sc.pickleFile(
    '/peaks/weekly_presence-' + "%s-%s" % (region, timeframe)).collectAsMap()

chiamate_orarie = sc.pickleFile(
    '/peaks/hourly_presence-' + "%s-%s" % (region, timeframe))

suffix = spatial_division.split('/')[-1]
peaks = open('peaks-%s-%s-%s' % (region, timeframe, suffix), 'w')
for l in chiamate_orarie.collect():
    print >>peaks, "%s,%s,%s,%s" % (l[0][0], l[0][4], l[0][3], l[
                                    1] / np.mean(list(presenze_medie[(l[0][0], l[0][1], l[0][3])])))
