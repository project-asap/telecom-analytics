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

import json
import datetime
import os
import sys

from dateutil import rrule

from pyspark import SparkContext

"""
Stats publisher module

It transform the results of sociometer module (i.e. composition of user of an area) into
json files compatible with weblyzard API

"""

ARG_DATE_FORMAT='%Y-%m-%d'

sc=SparkContext()

folder = sys.argv[1]
tag = sys.argv[2]
start_date = datetime.datetime.strptime(sys.argv[3], ARG_DATE_FORMAT)
end_date = datetime.datetime.strptime(sys.argv[4], ARG_DATE_FORMAT)

weeks = [d.isocalendar()[:2] for d in rrule.rrule(
    rrule.WEEKLY, dtstart=start_date, until=end_date
)]

prefix = 'sociometer-%s' % tag
file = open("%s.json" % prefix, "w")
for year, week in weeks:
    subfolder = "%s/%s/%s_%s" % (folder, tag, year, week)
    exists = os.system("$HADOOP_PREFIX/bin/hdfs dfs -test -e %s" % subfolder)
    if exists != 0:
        continue
    obs = []

    timeframe = (year, week)
    for w, p in enumerate(sc.textFile(subfolder).collect()):
        s = p.split(" ")
        target_location = s[0]
        value = s[-1]
        date = datetime.datetime.strptime(str(timeframe)+' 0', '(%Y, %W) %w')
        d = {}
        d["_id"] = "sociometer-%s"%(w)
        d["value"] = str(value).strip("\n")
        d["date"] = str(date)
        d["region_id"] = target_location
        d["description"] = s[1]  # d["target_location"]=[{"name":target_location,"point":{"lat":loc[0],"lon":loc[1].strip("\n")}}]
        d["indicator_id"] = "category_percentage"
        d["indicator_name"] = "category_percentage"
        obs.append(d)
    json.dump(obs, file)


