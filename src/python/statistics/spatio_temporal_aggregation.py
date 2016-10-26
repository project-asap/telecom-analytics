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

"""Spatio-temporal Aggregation module.

Given a CDR dataset and a set of geographical regions, it returns presences timeseries for each spatial region.
More specifically the results are tuples containing the following information:
<region>,<date>,<time>,<count>

Usage:
    $SPARK_HOME/bin/spark-submit --py-files cdr.py statistics/spatio_temporal_aggreation.py \
<dataset> <spatial_division> <start_date> <end_date>

Args:
    dataset: The dataset location. Can be any Hadoop-supported file system URI.
             The expected dataset schema is:
             user_id;null;null;start_date;start_time;duration;null;null;null;start_gsm_cell;end_gsm_cell;record_type
             The start_time column is expected to have this format: '%Y-%m-%d %X'.
    spatial_division: File containing the mapping of cells to regions.
    start_date: The starting date of the analysis (format: %Y-%m-%d)
    end_date: The ending date of the analysis (format: %Y-%m-%d)

Results are stored into the local file: timeseries-<region>-<start_date>-<end_date>
where the <region> is derived by the spatial_division.

Example:
    $SPARK_HOME/bin/spark-submit --py-files cdr.py statistics/spatio_temporal_aggreation.py \
hdfs:///dataset_simulated/2016 spatial_regions/aree_roma.csv 2016-01-01 2016-01-31
"""

__author__ = 'paul'

from pyspark import SparkContext, StorageLevel

import datetime
import string
import sys

from itertools import imap
from cdr import CDR


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
        .map(lambda x: (x.user_id,
                         x.region(cell2region),
                         x.date,
                         x.time[:2]),
                        ) \
        .distinct() \
        .persist(StorageLevel(False, True, False, False))

    hourly_presence = lines.map(
        lambda (_, region, date, time): (
            (region, date, time), 1)).reduceByKey(lambda x, y: x + y)

    area = spatial_division.split('/')[-1].split('.')[0]
    name = 'timeseries_%s_%s_%s' % (area,
                                   start_date.strftime(ARG_DATE_FORMAT),
                                   end_date.strftime(ARG_DATE_FORMAT))
    with open(name, 'w') as peaks:
        for (region, date, time), count in hourly_presence.collect():
            print >>peaks, "%s,%s,%s,%s" % (region, date, time, count)
