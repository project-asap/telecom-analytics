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
from pyspark import SparkContext
import time
import os
import sys
from cdr import CDR, Dataset#, check_complete_weeks_fast
from dateutil import rrule
from utils import quiet_logs

"""
ICP extraction (user profiling)

Given a CDR dataset and a set of geographical regions, it returns user profiles
for each spatial region. The results are tuples containing the following information:
<region>,<user_id>,<profile>.
More specifically, The analysis is divided in 4 week windows. Each window is divided in weeks,
weekdays or weekends, and 3 timeslots:
t0=[00:00-08:00], t1=[08:00-19:00], t2=[19:00-24:00].
The <profile> is a 24 element list containing the sum of user calls for such a division.
The column index for each division is: <week_idx> * 6 + <is_weekend> * 3 + <timeslot>
where <is_weekend> can be 0 or 1.

Usage:
    $SPARK_HOME/bin/spark-submit --py-files cdr.py user_profiling.py <dataset path> \
        <spatial region file> <format file> <format date> <area identifier>

Parameter:
    dataset path: path on hdfs where the dataset is located
    spatial region file: csv files with the association GSM antenna --> spatial region
    field2col: file containing the format of the csv files composing the dataset
    format date: the date format (according to python datetime module) of the cdr
    tag: a string used to name the results files
    start_date: The analysis starting date. Expected input %Y-%m-%d
    end_date: The analysis ending date. Expected input %Y-%m-%d

Example:
      $SPARK_HOME/bin/spark-submit --py-files cdr.py user_profiling.py dataset_simulated ../spatial_regions/aree_roma.csv field2col_simulated.csv %Y-%m-%d roma

Output: profiles will be saved in this path: /profiles/<area identifier>-<year>_<week>. Week is the first week of the corresponding
4 weeks block analyzed. These two parameters are computed inside the script, by scanning all the dataset and assigning each file to the
corresponding week.
"""

ARG_DATE_FORMAT='%Y-%m-%d'

########################functions##################################
def normalize(profilo):
    # normalize calls made on weekend or weekdays

    return [(region, week_idx, is_we, day_time, count * 1.0 / (2 if is_we == 1 else 5))
            for region, week_idx, is_we, day_time, count in profilo]


def array_carretto(profilo, weeks, user_id):
    # flll the list of calls in the basket with zeros where there are no dataV
    for munic in set([x[0] for x in profilo]):
        # settimana, work/we,timeslice, count normalizzato

        obs = [x[1:] for x in profilo if x[0] == munic]
        obs = sorted(obs, key=lambda d: sum(
            [j[3] for j in obs if j[0] == d[0]]), reverse=True)

        carr = [0 for x in range(len(weeks) * 2 * 3)]

        for w, is_we, t, count in obs:
            idx = (w - 1) * 6 + is_we * 3 + t
            carr[idx] = count
        yield munic, user_id, carr


##########################################################################
folder = sys.argv[1]
spatial_division = sys.argv[2]
field2col=sys.argv[3]
date_format=sys.argv[4]
tag=sys.argv[5]
start_date = datetime.datetime.strptime(sys.argv[6], ARG_DATE_FORMAT)
end_date = datetime.datetime.strptime(sys.argv[7], ARG_DATE_FORMAT)

# spatial division: cell_id->region of interest

file = open(spatial_division)
# converting cell to municipality
cell2municipi = {
    k: v for k,
    v in [
        (x.split(';')[0],
         x.split(';')[1].replace(
            "\n",
            "")) for x in file.readlines()]}

sc = SparkContext()
quiet_logs(sc)

start = time.time()
rddlist = []

field2col = {k: int(v) for k, v in [x.strip().split(',') for x in open(field2col)]}

#files, weeks = check_complete_weeks_fast(
#    folder,
#    lambda (n, d): d['type'] == 'FILE', # keep only files
#    lambda n: datetime.datetime.strptime(n.split('.')[0].split('_')[-1], '%Y%m%d').isocalendar()[:-1]
#)

weeks = [d.isocalendar()[:2] for d in rrule.rrule(
    rrule.WEEKLY, dtstart=start_date, until=end_date
)]

data = sc.textFile(folder) \
    .map(lambda row: CDR.from_string(row, field2col, date_format)) \
    .filter(lambda x: x is not None) \
    .filter(lambda x: x.valid_region(cell2municipi)) \
    .cache()

#for t in weeks[::2]:
for t in weeks[::4]:
    idx = weeks.index(t)
    window = weeks[idx:idx + 4]
    dataset = Dataset(data.filter(lambda x: x.week in window))
    r = dataset.data.map(lambda x: (x.user_id, x.region(cell2municipi),
                                     window.index(x.week), x.is_we(),
                                     x.day_of_week(), x.day_time())) \
        .distinct() \
        .map(lambda (user_id, region, week_idx, is_we, day_of_week, day_time):
             ((user_id, region, week_idx, is_we, day_time), 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .map(lambda  ((user_id, region, week_idx, is_we, day_time), count):
             (user_id, [[region, week_idx, is_we, day_time, count]])) \
        .reduceByKey(lambda x, y: x + y)

    #r = r.map(lambda (user_id, l): (user_id, sorted(l, key=lambda w: (
    #    w[0], sum([z[4] for z in l if z[1] == w[1]])), reverse=True)))

    r = r.map(lambda (user_id, l): (user_id, normalize(l)))
    r = r.flatMap(
        lambda user_id_l: array_carretto(
            user_id_l[1],
            window,
            user_id_l[0]))

    output = '/profiles/%s/%s' % (tag, '_'.join(map(str, window[0])))
    os.system("$HADOOP_HOME/bin/hadoop fs -rm -r %s" % output)
    r.saveAsTextFile(output)

print "elapsed time", time.time() - start
