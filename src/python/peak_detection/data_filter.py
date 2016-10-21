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

import datetime
from pyspark import SparkContext, StorageLevel

from itertools import imap, ifilter
from cdr import CDR

import os
import sys
import string

"""
Data Filter Module

Given a CDR dataset and a set of geographical regions, it returns the hourly presence for each region.

Usage: data_filter.py <folder> <spatial_division> <region> <timeframe>

--folder: hdfs folder where the CDR dataset is placed
--spatial division: csv file with the format GSM tower id --> spatial region
--region,timeframe: file name desired for the stored results. E.g. Roma 11-2015

example: pyspark data_filter.py dataset_simulated/06 ../spatial_regions/aree_roma.csv roma 06-2015

Results are stored into hdfs: /peaks/hourly_presence-<region>-<timeframe>

"""

ARG_DATE_FORMAT = '%Y-%m-%d'

if __name__ == '__main__':
    folder = sys.argv[1]
    spatial_division = sys.argv[2]
    start_date = datetime.datetime.strptime(sys.argv[3], ARG_DATE_FORMAT)
    end_date = datetime.datetime.strptime(sys.argv[4], ARG_DATE_FORMAT)

    with open(spatial_division) as file:
        # converting cell to municipality
        cell2region = {k: v for k, v in [
            imap(string.strip, x.split(';')) for x in file.readlines()]}

    sc = SparkContext()
    #sc._conf.set('spark.executor.memory','24g') \
    #    .set('spark.driver.memory','24g'). \
    #    set('spark.driver.maxResultsSize','0')

    # TODO check for complete weeks in dataset

    r = sc.textFile(folder) \
        .map(lambda x: CDR.from_string(x)) \
        .filter(lambda x: x is not None) \
        .filter(lambda x: x.valid_region(cell2region)) \
        .filter(lambda x: start_date <= x.date <= end_date) \
        .map(lambda x: ((x.user_id,
                         x.region(cell2region),
                         x.day_of_week(),
                         x.time[:2],
                         x.date),
                        1)) \
        .distinct() \
        .reduceByKey(lambda x, y: x + y) \
        .persist(StorageLevel(True, False, False, False, 1))

    hourly_calls = r.map(
        lambda ((_, region, dow, hour, date), count):
            ((region, dow, hour, date), 1.0)) \
        .reduceByKey(lambda x, y: (x + y))

    region = spatial_division.split('/')[-1].split('_')[-1].split('.')[0]
    name = '/peaks/hourly_%s_%s_%s' % (
        region,
        start_date.strftime(ARG_DATE_FORMAT),
        end_date.strftime(ARG_DATE_FORMAT))
    os.system("$HADOOP_PREFIX/bin/hdfs dfs -rm -r %s" % name)
    hourly_calls.saveAsPickleFile(name)
