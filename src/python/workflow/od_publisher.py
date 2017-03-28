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

It transform the results of peak detection module (i.e. hourly presences computation given a spatial region) into
json files compatible with weblyzard API

Usage: peaks_publisher.py <spatial_division> <region> <timeframe>


--spatial division: csv file with the format GSM tower id --> spatial region
--region,timeframe: file name desired for the stored results. E.g. Roma 11-2015

Note: this sample is only valid for spatial regions <torri_roma.csv>

example: pyspark peaks_publisher.py  ../spatial_regions/torri_roma.csv roma 06-2015

Results are stored into file: timeseries-<region>-<timeframe>-<spatial_division>.csv
"""

folder = sys.argv[1]
peaks = open('od_timeseries-%s.csv' % folder)

obs = []

for i, p in enumerate(peaks.readlines()):
	region, date, origin, value = p.split(";")
	date = datetime.datetime.strptime(date, '%Y%m%d')
	d = {}
	d["_id"] = "od-%s"%(i)
	d["value"] = str(value).strip("\n")
	d["date"] = str(date)
	d["region_id"] = region
	d["description"] = origin  # d["target_location"]=[{"name":target_location,"point":{"lat":loc[0],"lon":loc[1].strip("\n")}}]
	d["indicator_id"] = "origin_destination"
	d["indicator_name"] = "origin_destination"
	obs.append(d)
file = open("observation-od-%s.json" % folder, "w")
json.dump(obs, file)
