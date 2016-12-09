__author__ = 'paul'
import string
import sys

from itertools import imap
from cdr import CDR
from utils import quiet_logs

from pyspark import SparkContext, StorageLevel

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

folder = sys.argv[1]
spatial_division = sys.argv[2]
region = sys.argv[3]
timeframe = sys.argv[4]

# spatial division: cell_id->region of interest
with open(spatial_division) as file:
    # converting cell to municipality
    cell2region = {k: v for k, v in [
        imap(string.strip, x.split(';')) for x in file.readlines()]}

sc = SparkContext()
quiet_logs(sc)

peaks = open('timeseries-%s-%s-%s' %
             (region, timeframe, spatial_division.split("/")[-1]), 'w')

lines = sc.textFile(folder) \
    .map(lambda x: CDR.from_string(x)) \
    .filter(lambda x: x is not None) \
    .filter(lambda x: x.valid_region(cell2region)) \
    .map(lambda x: (x.user_id, x.region(cell2region), x.date, x.time[:2] )) \
    .distinct() \
    .persist(StorageLevel(False, True, False, False))

chiamate_orarie = lines.map(
    lambda __region_date_time: (
        (__region_date_time[1],
         __region_date_time[2],
         __region_date_time[3]),
        1)).reduceByKey( lambda x, y: x + y)
for (region, date, time), count in chiamate_orarie.collect():
    print >>peaks, "%s,%s,%s,%s" % (region, date, time, count)
