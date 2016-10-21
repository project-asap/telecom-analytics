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

import re
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
if __name__ == '__main__':
    folder = sys.argv[1]

    sc = SparkContext()

    hourly_calls = sc.pickleFile(folder)

    # format: region, weekday, hour
    mean_presence = hourly_calls.map(
        lambda ((region, dow, hour, start_date), count): (
            (region, dow, hour), count)
    ).groupByKey()

    name = folder.replace('hourly', 'weekly')
    os.system("$HADOOP_PREFIX/bin/hadoop fs -rm -r %s" % name)
    mean_presence.saveAsPickleFile(name)
