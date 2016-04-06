import json
import sys

from datetime import datetime
from time import mktime

"""
Stats publisher module

It transform the results of peak detection module (i.e. hourly presences computation given a spatial region) into
json files compatible with weblyzard API

Usage: peaks_publisher.py <spatial_division> <region> <timeframe>


--spatial division: csv file with the format GSM tower id --> spatial region
--region,timeframe: file name desired for the stored results. E.g. Roma 11-2015

Note: this sample is only valid for spatial regions <torri_roma.csv>

example: pyspark stats_publisher.py  ../spatial_regions/torri_roma.csv roma 06-2015

Results are stored into file: timeseries-<region>-<timeframe>-<spatial_division>.csv
"""

spatial_division = sys.argv[1]
region = sys.argv[2]
timeframe = sys.argv[3]

from_date_format = '%Y-%m-%d'
id_to_name = lambda id: id.replace("_", " ")
to_timestamp = lambda d: int(mktime(d.timetuple()))

with open('peaks-%s-%s-%s' % (region,
                              timeframe,
                              spatial_division.split('/')[-1])) as peaks:
    obs = []

    for i, p in enumerate(peaks.readlines()):
        target_location, date_str, hour_str, value = p.split(",")
        date = datetime.strptime(date_str, from_date_format)
        hour = int(hour_str)
        d = {}
        d["value"] = str(value).strip("\n")
        d["date"] = to_timestamp(date.replace(hour=hour))
        d["added_date"] = to_timestamp(datetime.now())
        d["location_id"] = target_location
        d["indicator_id"] = "Peaks"
        d["_id"] = "%s_%s" % (d["indicator_id"], i)
        d["indicator_name"] = id_to_name(d["indicator_id"])
        d["uri"] = 'http://example.com/%s/%s' % (d["indicator_id"], d["_id"])
        obs.append(d)

with open("peaks-%s-%s-%s.json" % (region, timeframe, spatial_division.split("/")[-1]), "w") as file:
    json.dump(obs, file)
