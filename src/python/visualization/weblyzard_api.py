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
import json
import datetime
import sys

spatial_division = sys.argv[1]
region = sys.argv[2]
timeframe = sys.argv[3]

with open('roma_gsm.csv') as geo:
    coord = {x.split(";")[0][:5]: (x.split(";")[1:]) for x in geo.readlines()}

with open('../statistics/timeseries%s-%s-%s.csv' %
          (region, timeframe, spatial_division.split("/")[-1])) as peaks:
    obs = []

    for i, p in enumerate(peaks.readlines()):
        s = p.split(",")
        target_location = s[0]
        loc = coord[s[0]]
        value = s[-1]
        date = d = datetime.datetime.strptime(s[1] + s[2], ' %Y-%m-%d %H')
        d = {}
        d["_id"] = i
        d["value"] = str(value).strip("\n")
        d["date"] = str(date)
        d["target_location"] = [{"name": target_location, "point": {
            "lat": loc[0], "lon":loc[1].strip("\n")}}]
        d["indicator_id"] = "hourly presence"
        d["indicator_name"] = "hourly presence"
        obs.append(d)

with open("wl_area_presence.json", "w") as file:
    json.dump(obs, file)
