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
import sys

"""
Stats publisher module

It transform the results of origin-destination matrix module into
json files compatible with weblyzard API


"""

tag = sys.argv[1]

#weeks_dict=check_complete_weeks_fast(folder)
w=0


peaks = open('od_timeseries-%s.csv' % tag)

obs = []

for i, p in enumerate(peaks.readlines()):

	s = p.split(";")
	origin = s[0]
	value = s[-1]
	date = datetime.datetime.strptime(s[1], '%Y-%m-%d %X')
	d = {}
	d["_id"] = "od-%s"%(w)
	d["value"] = str(value).strip("\n")
	d["date"] = str(date)
	d["region_id"] = s[2]
	d["description"] = origin  # d["target_location"]=[{"name":target_location,"point":{"lat":loc[0],"lon":loc[1].strip("\n")}}]
	d["indicator_id"] = "origin_destination"
	d["indicator_name"] = "origin_destination"
	obs.append(d)
	w+=1
file = open("od-%s.json" % tag, "w")
json.dump(obs, file)


