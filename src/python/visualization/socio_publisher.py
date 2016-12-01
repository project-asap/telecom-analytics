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

region = sys.argv[1]
timeframe = sys.argv[2]

peaks = open('sociometer-%s-%s' % (region, timeframe))

obs = []

for i, p in enumerate(peaks.readlines()):
    s = p.split(" ")
    target_location = s[0]
    value = s[-1]
    date = d = datetime.datetime.strptime(timeframe, '%m-%Y')
    d = {}
    d["_id"] = i
    d["value"] = str(value).strip("\n")
    d["date"] = str(date)
    d["region_id"] = target_location
    d["profile_type"] = s[1]  # d["target_location"]=[{"name":target_location,"point":{"lat":loc[0],"lon":loc[1].strip("\n")}}]
    d["indicator_id"] = "hourly presence percentage"
    d["indicator_name"] = "hourly presence percentage"
    obs.append(d)

file = open("sociometer%s-%s.json" % (region, timeframe), "w")
json.dump(obs, file)

	