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

#user_annotation = sc.textFile(user2label, minPartitions=60000) \
user_annotation = sc.textFile(user2label, minPartitions=6000) \
    .map(lambda e: eval(e))
user_cache = {}

def get_class(users, region):
    rest = [u for u in users if (u, region) not in user_cache]
    broadcasted = sc.broadcast(rest)
    print '<<< <<<<', len(rest), len(users)
    d = user_annotation.filter(lambda ((u, r), _): u in broadcasted.value and r == region) \
        .collectAsMap()
    user_cache.update(d)

#def get_user_class(user, region):
#    if (user, region) in user_cache:
#        user_cache[(user, region)]
#    else:
#        label = user_annotation.filter(lambda ((u, r), _): u == user and r == region) \
#            .collectAsMap().get((user, region), 'not classified')
#        user_cache[(user, region)] = label

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

results = {}
out_file = open("presence_timeseries-%s.csv" % area, "w")
for f in files:
    user_calls_per_region = sc.textFile(f, minPartitions=540) \
        .map(lambda row: CDR.from_string(row, field2col, date_format)) \
        .filter(lambda x: x is not None) \
        .filter(lambda x: x.start_cell[:5] in sites2zones) \
        .map(lambda x: (sites2zones[x.start_cell[:5]], x.date, x.user_id)) \
        .distinct() \
        .map(lambda (region, date, user_id): ((region, date), [user_id])) \
        .reduceByKey(lambda l1, l2: l1 + l2) \
        .collect()

    for (region, date), users in user_calls_per_region:
        print '>>> >>>', region, date
        get_class(users, region)
        class_per_region = [((user_cache.get((user, region), 'not classified'),
                              region,
                              date),
                             1) for user in users]
        d = sc.parallelize(class_per_region) \
            .reduceByKey(lambda x, y: x + y) \
            .collectAsMap()
        print '<<<<', results.keys(), d.keys()
        for (class_label, region, date), count in d.iteritems():
            print >> out_file, "%s;%s;%s;%s"%(region, date, class_label, count)
        results.update({k: results.get(k, 0) + v for k, v in d.iteritems() })

print '<<<< <<<<', results
