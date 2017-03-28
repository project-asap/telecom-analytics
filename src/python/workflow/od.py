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

from pyspark import SparkContext
import sys
from cdr import CDR

from utils import quiet_logs

(interest_region,
 user2label,
 field2col,
 date_format,
 folder,
 tag) = sys.argv[1:7]


sc=SparkContext()
#quiet_logs(sc)

user_origin = sc.textFile('/'.join([user2label, tag])) \
    .map(lambda e: eval(e)) \
    .filter(lambda ((user, region), user_class): user_class in ['resident', 'dynamic_resident']) \
    .map(lambda ((user, region), _): (user, region))

sites2zones={}
f=open(interest_region)
for row in f:
    row = row.strip().split(';')
    sites2zones[row[0][:5]] = row[1]

field2col = {k: int(v) for k, v in [x.strip().split(',') for x in open(field2col)]}

zone = lambda x: sites2zones[x.start_cell[:5]]

results = sc.textFile(folder) \
    .map(lambda row: CDR.from_string(row, field2col, date_format)) \
    .filter(lambda x: x is not None) \
    .filter(lambda x: x.start_cell[:5] in sites2zones) \
	.map(lambda x: (x.user_id, (zone(x), x.date))) \
    .distinct() \
    .leftOuterJoin(user_origin) \
    .map(lambda (user, ((dest, date), origin)):
         ((origin if origin != None else 'outbound', dest, date), 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .collect()

out_file=open("od_timeseries-%s.csv" % tag, "w")
for r in results:
    print >> out_file,  "%s;%s;%s;%s"%(r[0][1], r[0][2], r[0][0], r[1])
