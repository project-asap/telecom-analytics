import datetime
import json
import string
import sys

from itertools import imap
import numpy as np
from sets import Set

"""Stats publisher module

It transform the results of stats module (i.e. hourly presences computation given a spatial region) into
json files compatible with weblyzard API

Usage:
    python visualization/start_publisher.py <timeseries> <gsm> <region> <timeframe>

Args:
    timeseries: The timeseries file location.
                The expected schema is:
                <region>,<date>,<time>,<count>,<start-cells>
    gsm: The gsm file location. The expected schema is:
         <cell>;<latitude>;<longitude>
    region: The region name featuring in the stored results
    timeframe: The timeframe featuring in the stored results

Results are stored into the local file file: wl_timeseries-<region>-<timeframe>.json

Example:
    python visualization/stats_publisher.py spatial_regions/roma_gsm.csv roma 06-2015
"""

timeseries_location = sys.argv[1]
gsm = sys.argv[2]
region = sys.argv[3]
timeframe = sys.argv[4]

with open(gsm) as f:
    coord = {cell: np.array(map(float, [latitude, longitude])) for cell, latitude, longitude in [
        imap(string.strip, l.split(';')) for l in f.readlines()[1:]]}

obs = []
with open(timeseries_location) as timeseries:
    for i, (area, date, time, count, start_cells) in enumerate(
            p.strip().split(',', 4) for p in timeseries.readlines()):
        lat, lon = np.median([coord[c] for c in eval(start_cells)], axis=0)
        d = {"_id": i,
             "value": count,
             "date": str(datetime.datetime.strptime(date, '%Y-%m-%d %X') +
                         datetime.timedelta(hours=int(time))),
             "target_location": [{"name": area, "point": {"lat": lat, "lon": lon}}],
             "indicator_id": "hourly presence",
             "indicator_name": "hourly presence"}
        obs.append(d)

file = open("wl_timeseries%s-%s.json" %
            (region, timeframe), "w")
json.dump(obs, file)
