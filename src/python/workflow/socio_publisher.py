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
import json
import os

folder, region = sys.argv[1], sys.argv[2]


for f in filter(lambda f: f.startswith('sociometer-%s' % region),
                os.listdir(folder)):
    peaks = open(os.path.join(folder, f))
    timeframe = f.split('-')[-1].split('_')
    obs = []

    for w, p in enumerate(peaks.readlines()):
        target_location, desc, value = p.split(" ")
        date = datetime.datetime.strptime(str(timeframe), '(%Y, %W)')
        d = {}
        d["_id"] = "sociometer-%s"%(w)
        d["value"] = str(value).strip("\n")
        d["date"] = str(date)
        d["region_id"] = target_location
        d["description"] = desc  # d["target_location"]=[{"name":target_location,"point":{"lat":loc[0],"lon":loc[1].strip("\n")}}]
        d["indicator_id"] = "category_percentage"
        d["indicator_name"] = "category_percentage"
        obs.append(d)
        w+=1
    file = open("observation-sociometer%s-%s_%s.json" % (region, timeframe[0], timeframe[1]), "w")
    json.dump(obs, file)


