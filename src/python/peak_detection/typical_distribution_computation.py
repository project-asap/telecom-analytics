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

import os
import sys

"""
Typical Distribution Computation Module

Given a hourly presence dataset (usually regarding a month of activity), it aggregates the presences according to week days and hours.

Usage: typical_distribution_computation.py  <region> <timeframe>

--region,timeframe: names of the file stored into the hdfs. E.g. Roma 11-2015

example: pyspark typical_distribution_computation.py  roma 06-215

It loads the hourly presences in /peaks/weekly_presence-<region>-<timeframe> and stores
results into hdfs: /peaks/weekly_presence-<region>-<timeframe>

"""


########################functions##################################
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


region = sys.argv[1]
timeframe = sys.argv[2]

# spatial division: cell_id->region of interest


# data loading
# checking file existance
#####
sc = SparkContext()


chiamate_orarie = sc.pickleFile(
    '/peaks/hourly_presence-' + "%s-%s" % (region, timeframe))
presenze_medie = chiamate_orarie.map(lambda x: (
    (x[0][0], x[0][1], x[0][3]), x[1])).groupByKey()
os.system("$HADOOP_HOME/bin/hadoop fs -rm -r /peaks/weekly_presence-%s-%s/" %
          (region, timeframe))
presenze_medie.saveAsPickleFile(
    '/peaks/weekly_presence-' + "%s-%s" % (region, timeframe))


##picchi ##
