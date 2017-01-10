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

#weeks_dict=check_complete_weeks_fast(folder)
w=0


peaks = open('results/od_timeseries-%s.csv'%folder)

obs = []

for i, p in enumerate(peaks.readlines()):

	s = p.split(";")
	origin = s[0]
	value = s[-1]
	date = datetime.datetime.strptime(s[1], '%Y%m%d')
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
file = open("observation/od-%s.json"%folder, "w")
json.dump(obs, file)

    
