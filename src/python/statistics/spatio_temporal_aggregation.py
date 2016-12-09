__author__ = 'paul'
import datetime
from pyspark import SparkContext,StorageLevel

import sys

"""
Spatio-temporal Aggregation module

Given a CDR dataset and a set of geographical regions, it returns presences timeseries for each spatial region.

Usage: spatio_temporal_aggreation.py <folder> <spatial_division> <region> <timeframe>

--folder: hdfs folder where the CDR dataset is placed
--spatial division: csv file with the format GSM tower id --> spatial region
--region,timeframe: file name desired for the stored results. E.g. Roma 11-2015

example: pyspark spatio_temporal_aggreation.py dataset_simulated/06 ../spatial_regions/aree_roma.csv roma 06-2015

Results are stored into file: timeseries-<region>-<timeframe>-<spatial_division>.csv

"""


########################functions##################################
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def validate(date_text, date_format='%Y-%m-%d %X'):
### if the string is a date, return True (useful to filter csv header)
    date_text = date_text.strip()
    try:
        datetime.datetime.strptime(date_text, date_format)
        return True
    except ValueError:
        return False

def municipio(cell_id, cell2municipi):
	try:
		cell2municipi[cell_id]
		return True
	except KeyError:
		return False


##########################################################################################
folder=sys.argv[1]
spatial_division=sys.argv[2]
region=sys.argv[3]
timeframe=sys.argv[4]

###spatial division: cell_id->region of interest

file=open(spatial_division)
#converting cell to municipality
cell2municipi={k:v for k,v in [(x.split(';')[0].replace(" ",""),x.split(';')[1].replace("\n","")) for x in file.readlines()]}

sc = SparkContext()
quiet_logs(sc)

peaks=open('timeseries-%s-%s-%s'%(region,timeframe,spatial_division.split("/")[-1]),'w')

lines = sc.textFile(folder) \
    .filter(lambda x: validate(x.split(';')[3])) \
    .filter(lambda x: municipio(x.split(';')[9].replace(" ",""), cell2municipi)) \
    .map(lambda x: (x.split(';')[0],cell2municipi[x.split(';')[9].replace(" ","")], x.split(';')[3], x.split(';')[4][:2] )) \
    .distinct() \
    .persist(StorageLevel(False, True, False, False))

chiamate_orarie = lines.map(lambda x: ((x[1],x[2], x[3]), 1)).reduceByKey(lambda x, y: x + y)
for l in  chiamate_orarie.collect():
    print >>peaks, "%s,%s,%s,%s"%(l[0][0],l[0][1],l[0][2],l[1])
