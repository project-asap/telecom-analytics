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

from pyspark import SparkContext, StorageLevel

import datetime
import string
import sys

from itertools import imap
from cdr import CDR

"""
Spatio-temporal Aggregation module

Given a CDR dataset and a set of geographical regions, it returns presences timeseries for each spatial region.

Usage: spatio_temporal_aggreation.py <folder> <spatial_division> <region> <timeframe>

--folder: hdfs folder where the CDR dataset is placed
--spatial division: csv file with the format GSM tower id --> spatial region
--region,timeframe: file name desired for the stored results. E.g. Roma 11-2015

example: pyspark spatio_temporal_aggreation.py dataset_simulated/06 ../spatial_regions/aree_roma.csv roma 06-2015

Results are stored into file: timeseries-<region>-<timeframe>-<spatial_division>.csv

"""

ARG_DATE_FORMAT = '%Y-%m-%d'

if __name__ == '__main__':
    folder = sys.argv[1]
    spatial_division = sys.argv[2]
    start_date = datetime.datetime.strptime(sys.argv[3], ARG_DATE_FORMAT)
    end_date = datetime.datetime.strptime(sys.argv[4], ARG_DATE_FORMAT)

    # spatial division: cell_id->region of interest
    with open(spatial_division) as file:
        # converting cell to municipality
        cell2region = {k: v for k, v in [
            imap(string.strip, x.split(';')) for x in file.readlines()]}

    sc = SparkContext()
    lines = sc.textFile(folder) \
        .map(lambda x: CDR.from_string(x)) \
        .filter(lambda x: x is not None) \
        .filter(lambda x: x.valid_region(cell2region)) \
        .filter(lambda x: start_date <= x.date <= end_date) \
        .map(lambda x: ((x.user_id,
                         x.region(cell2region),
                         x.date,
                         x.time[:2]),
                        1)) \
        .distinct() \
        .reduceByKey(lambda x, y: x + y) \
        .persist(StorageLevel(False, True, False, False))

    hourly_presence = lines.map(
        lambda ((user_id, region, date, time), _): (
            (region, date, time), 1)).reduceByKey(lambda x, y: x + y)

    area = spatial_division.split('/')[-1].split('.')[0]
    name = 'timeseries_%s_%s_%s' % (area,
                                   start_date.strftime(ARG_DATE_FORMAT),
                                   end_date.strftime(ARG_DATE_FORMAT))
    with open(name, 'w') as peaks:
        for (region, date, time), count in hourly_presence.collect():
            print >>peaks, "%s,%s,%s,%s" % (region, date, time, count)
