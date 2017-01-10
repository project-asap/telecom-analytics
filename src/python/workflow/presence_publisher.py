import json
import datetime
import sys

"""
Stats publisher module

It transform the results of peak detection module (i.e. hourly presences computation given a spatial region) into
json files compatible with weblyzard API


"""

folder = sys.argv[1]

#weeks_dict=check_complete_weeks_fast(folder)
w=0


peaks = open('results/presence_timeseries-%s.csv'%folder)

obs = []

for i, p in enumerate(peaks.readlines()):

	s = p.split(";")
	target_location = s[0]
	value = s[-1]
	date = datetime.datetime.strptime(s[1], '%Y%m%d')
	d = {}
	d["_id"] = "presence-%s"%(w)
	d["value"] = str(value).strip("\n")
	d["date"] = str(date)
	d["region_id"] = target_location
	d["description"] = s[2]  # d["target_location"]=[{"name":target_location,"point":{"lat":loc[0],"lon":loc[1].strip("\n")}}]
	d["indicator_id"] = "area_presence"
	d["indicator_name"] = "area_presence"
	obs.append(d)
	w+=1
file = open("observation/area_presence%s.json"%folder, "w")
json.dump(obs, file)

    
