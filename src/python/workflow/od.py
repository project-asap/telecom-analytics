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
import sys

from pyspark import SparkContext

from cdr import CDR, explore_input
from utils import quiet_logs

(interest_region,
 user2label,
 field2col,
 date_format,
 folder,
 area) = sys.argv[1:7]

sc = SparkContext()
quiet_logs(sc)
#sc._conf.set('spark.executor.memory','32g') \
#    .set('spark.driver.memory','32g') \
#    .set('spark.driver.maxResultsSize','0')

user_annotation = sc.textFile(user2label).map(lambda e: eval(e))
user_cache = {}
def get_home(user):
    if user in user_cache:
        return user_cache[user]
    else:
        home = user_annotation \
        .filter(lambda ((u, _), user_class): user == u and user_class == 'resident') \
        .map(lambda ((u, r), _): (u, r)) \
        .collectAsMap().get(user, "outbound")
        user_cache[(user, region)] = home
        return home

sites2zones = {}
f = open(interest_region)
for row in f:
    row = row.strip().split(';')
    sites2zones[row[0][:5]] = row[1]

field2col = {k: int(v) for k, v in [x.strip().split(',') for x in open(field2col)]}

files, _ = explore_input(
    folder,
    lambda (n, d): d['type'] == 'FILE', # keep only files
    lambda n: datetime.datetime.strptime(n.split('.')[0].split('_')[-1], '%Y%m%d').isocalendar()[:-1]
)

user_calls_per_region = sc.textFile(','.join(files)) \
    .map(lambda row: CDR.from_string(row, field2col, date_format)) \
    .filter(lambda x: x is not None) \
    .filter(lambda x: x.start_cell[:5] in sites2zones) \
    .map(lambda x: ((sites2zones[x.start_cell[:5]], x.date), set([x.user_id]))) \
    .reduceByKey(lambda s1, s2: s1 | s2) \
    .collect()

home_per_region = [((get_home(user), region, date), 1)
                      for (region, date), users in user_calls_per_region for user in users]

results = sc.parallelize(home_per_region) \
    .reduceByKey(lambda x, y : x + y) \
    .collect()

out_file=open("od_timeseries-%s.csv" % area, "w")
for ((home, region, date), count) in results:
    print >> out_file, "%s;%s;%s;%s"%(region, date, home, count)
