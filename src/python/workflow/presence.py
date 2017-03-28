#
# Copyright 2015-2017 ASAP
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

from cdr import CDR
from utils import quiet_logs

(interest_region,
 user2label,
 field2col,
 date_format,
 folder,
 tag) = sys.argv[1:7]

sc = SparkContext()
quiet_logs(sc)

#user_annotation = sc.textFile(user2label, minPartitions=60000) \
user_annotation = sc.textFile('/'.join([user2label, tag])) \
    .map(lambda e: eval(e))

sites2zones = {}
f = open(interest_region)
for row in f:
    row = row.strip().split(';')
    sites2zones[row[0][:5]] = row[1]

field2col = {k: int(v) for k, v in [x.strip().split(',') for x in open(field2col)]}

zone = lambda x: sites2zones[x.start_cell[:5]]

out_file = open("presence_timeseries-%s.csv" % tag, "w")
l = sc.textFile(folder) \
    .map(lambda row: CDR.from_string(row, field2col, date_format)) \
    .filter(lambda x: x is not None) \
    .filter(lambda x: x.start_cell[:5] in sites2zones) \
    .map(lambda x: ((x.user_id, zone(x)), x.date)) \
    .leftOuterJoin(user_annotation) \
    .map(lambda ((user_id, region), (date, user_class)):
         ((user_class if user_class != None else 'not classified', region, date), 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .collect()

for (class_label, region, date), count in l:
    print >> out_file, "%s;%s;%s;%s"%(region, date, class_label, count)
