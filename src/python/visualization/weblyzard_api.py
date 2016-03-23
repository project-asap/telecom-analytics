import json
import datetime
import sys

spatial_division = sys.argv[1]
region = sys.argv[2]
timeframe = sys.argv[3]

geo = open('roma_gsm.csv')
coord = {x.split(";")[0][:5]: (x.split(";")[1:]) for x in geo.readlines()}

peaks = open('../statistics/timeseries%s-%s-%s.csv' %
             (region, timeframe, spatial_division.split("/")[-1]))

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

file = open("wl_area_presence.json", "w")
json.dump(obs, file)
