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

from pyspark import SparkContext
import os
import string
import sys

from dateutil import rrule
from itertools import imap
from cdr import *

"""
User Profiling Module

Given a CDR dataset and a set of geographical regions, it returns user profiles for each spatial region.

Usage: user_profiling.py <folder> <spatial_division> <region> <timeframe>

--folder: hdfs folder where the CDR dataset is placed
--spatial division: csv file with the format GSM tower id --> spatial region
--region,timeframe: file name desired for the stored results. E.g. Roma 11-2015

example: pyspark user_profiling.py dataset_simulated/06 ../spatial_regions/aree_roma.csv roma 06-2015

Results are stored into hdfs: /peaks/profiles-<region>-<timeframe>

"""

ARG_DATE_FORMAT = '%Y-%m-%d'

########################functions##################################

def array_carretto(profilo, weeks, user_id):
    # flll the list of calls in the basket with zeros where there are no data
    for munic in set([x[0] for x in profilo]):
        # settimana, work/we,timeslice, count normalizzato

        obs = [x[1:] for x in profilo if x[0] == munic]
        print('obs:' % obs)
        obs = sorted(obs, key=lambda d: sum(
            [j[3] for j in obs if j[0] == d[0]]), reverse=True)
        print('>>> obs:' % obs)

        carr = [0 for x in range(len(weeks) * 2 * 3)]

        for w, is_we, t, count in obs:
            idx = (w - 1) * 6 + is_we * 3 + t
            carr[idx] = count
        yield munic, user_id, carr


def normalize(profilo):
    # normalizza giorni chiamate su week end e  workday
    return [(region, week_idx, is_we, day_time, count * 1.0 / (2 if is_we == 1 else 5)) for
            region, week_idx, is_we, day_time, count in profilo]

if __name__ == '__main__':
    folder = sys.argv[1]
    spatial_division = sys.argv[2]
    start_date = datetime.datetime.strptime(sys.argv[3], ARG_DATE_FORMAT)
    end_date = datetime.datetime.strptime(sys.argv[4], ARG_DATE_FORMAT)

    weeks = [d.isocalendar()[:2] for d in rrule.rrule(
        rrule.WEEKLY, dtstart=start_date, until=end_date)]

    # spatial division: cell_id->region of interest
    with open(spatial_division) as file:
        # converting cell to municipality
        cell2region = {k: v for k, v in [
            imap(string.strip, x.split(';')) for x in file]}

    #####
    sc = SparkContext()
    data = sc.textFile(folder) \
        .map(lambda row: CDR.from_string(row)) \
        .filter(lambda x: x is not None) \
        .filter(lambda x: x.valid_region(cell2region)) \
        .filter(lambda x: start_date <= x.date <= end_date) \

    for t in weeks[::4]:
        idx = weeks.index(t)
        if len(weeks[idx:idx + 4]) < 4:
            print('No complete 4 weeks: %s' % (weeks[idx:idx + 4]))
            continue
        year, week = t
        dataset = Dataset(data.filter(lambda x: x.week in weeks[idx:idx + 4]))
        start_week = '_'.join(map(str, weeks[idx]))
        end_week = '_'.join(map(str, weeks[idx + 3] if idx + 3 < len(weeks) else weeks[-1]))

        r = dataset.data.map(lambda x: ((x.user_id, x.region(cell2region), weeks.index(x.week), x.is_we(), x.day_of_week(), x.day_time(), x.year()), 1)) \
            .distinct() \
            .map(lambda ((user_id, region, week_idx, is_we, day_of_week, day_time, year), _):
                 ((user_id, region, week_idx, is_we, day_time), 1)) \
            .reduceByKey(lambda x, y: x + y) \
            .map(lambda ((user_id, region, week_idx, is_we, day_time), count):
                 (user_id, [[region, week_idx, is_we, day_time, count]])) \
            .reduceByKey(lambda x, y: x + y)

        ###
        # Carrello format: user -> [(region, settimana, weekend/workday, time_slice, count),...]
        # nota: count= day of presence in the region at the timeslice

        # week ordering
        # keys: region,busiest week,workday/we,timeslice
        r = r.map(lambda (user_id, l):
                  (user_id,
                   sorted(l, key=lambda (region, week_idx, is_we, day_time, count):
                          (region,
                           sum([count for _, wom, _, _, count in l if wom == week_idx]),
                           -is_we,
                           day_time),
                          reverse=True)))

        r = r.map(lambda (user_id, l): (user_id, normalize(l)))

        r = r.flatMap(lambda (user_id, l): array_carretto(l, weeks[idx:idx + 4], user_id))
        region = spatial_division.split('/')[-1].split('.')[0]
        name = '/profiles/%s-%s-%s' % (region, start_week, end_week)
        os.system("$HADOOP_HOME/bin/hadoop fs -rm -r %s" % name)
        r.saveAsPickleFile(name)
