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
def check_complete_weeks_fast(folder):
        """ week check with file names. 
        """
        weeks=defaultdict(int)
        for x in hdfs.ls("/"+"ttmetro/roma"):
            for f in hdfs.ls(x):
                try:
                    d=datetime.datetime.strptime(f.split("_")[6].split(".")[0], "%Y%m%d")
                    weeks[(d.isocalendar()[0],d.isocalendar()[1])]+=1
                except:
                    pass
        return [x for x in weeks if weeks[x]>=5]
folder,region = sys.argv[1],sys.argv[2]
import cPickle as pk
weeks=pk.load(open('weeks_dict.pkl'))
#weeks_dict=check_complete_weeks_fast(folder)
w=0
for timeframe in weeks:
    try:
        peaks = open('results/sociometer-%s-%s_%s' % (region, timeframe[0],timeframe[1]))
    except:
        continue
    obs = []

    for i, p in enumerate(peaks.readlines()):

        s = p.split(" ")
        print s
        target_location = s[0]
        value = s[-1]
        date = datetime.datetime.strptime(str(timeframe), '(%Y, %W)')
        d = {}
        d["_id"] = "sociometer-%s"%(w)
        d["value"] = str(value).strip("\n")
        d["date"] = str(date)
        d["region_id"] = target_location
        d["description"] = s[1]  # d["target_location"]=[{"name":target_location,"point":{"lat":loc[0],"lon":loc[1].strip("\n")}}]
        d["indicator_id"] = "category_percentage"
        d["indicator_name"] = "category_percentage"
        obs.append(d)
        w+=1
    file = open("observation/sociometer%s-%s_%s.json" % (region, timeframe[0],timeframe[1]), "w")
    json.dump(obs, file)

    
