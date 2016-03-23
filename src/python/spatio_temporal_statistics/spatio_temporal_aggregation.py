__author__ = 'paul'
import datetime
from pyspark import SparkContext, StorageLevel

import string as str_

from itertools import imap

import time
import sys

#DATE_FORMAT = '%Y%m%d'
DATE_FORMAT = '%Y-%m-%d'

MIN_PARTITIONS = 512

"""
Spatio-temporal Aggregation module

Given a CDR dataset and a set of geographical regions, it returns presences timeseries for each spatial region.

Usage: spatio_temporal_aggreation.py <folder> <spatial_division> <region> <timeframe>

--folder: hdfs folder where the CDR dataset is placed
--spatial division: csv file with the format GSM tower id --> spatial region
--region,timeframe: file name desired for the stored results. E.g. Roma 11-2015

example: pyspark spatio_temporal_aggreation.py /dataset_simulated/06 ../spatial_regions/aree_roma.csv roma 06-215

Results are stored into file: timeseries-<region>-<timeframe>-<spatial_division>.csv

"""


########################functions##################################
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def euclidean(v1, v2):
    print v1, v2
    return sum([abs(v1[i] - v2[i])**2 for i in range(len(v1))])**0.5


def validate(date_text):
    # if the string is a date, return True (useful to filter csv header)
    try:
        datetime.datetime.strptime(date_text, DATE_FORMAT)
        return True
    except ValueError:
        return False


def municipio(cell_id):
    try:
        cell2municipi[cell_id]
        return True
    except KeyError:
        return False


##########################################################################
folder = sys.argv[1]
spatial_division = sys.argv[2]
region = sys.argv[3]
timeframe = sys.argv[4]

# spatial division: cell_id->region of interest

with open(spatial_division) as file:
    # converting cell to municipality
    cell2municipi = {k: v for k, v in [(x.split(';')[0].replace(
        " ", ""), x.split(';')[1].replace("\n", "")) for x in file.readlines()]}

# data loading
# checking file existence
#####
sc = SparkContext()
quiet_logs(sc)

start = time.time()

lines = sc.textFile(folder, minPartitions=MIN_PARTITIONS).map(
    lambda l: list(imap(str_.strip, l.split(';'))))

# plot utenti con chiamate durante partite
lines = lines.filter(lambda x: validate(x[3])) \
    .filter(lambda x: municipio(x[9])) \
    .map(lambda x: ((x[1], cell2municipi[x[9]], x[3], x[4][:2] ), 1)) \
    .distinct() \
    .reduceByKey(lambda x, y: x + y) \
    .persist(StorageLevel(False, True, False, False))

chiamate_orarie = lines.map(lambda x: (
    (x[0][1], x[0][2], x[0][3]), 1)).reduceByKey(lambda x, y: x + y)

with open('timeseries%s-%s-%s.csv' % (
        region, timeframe, spatial_division.replace(".", "").replace("/", "")), 'a') as peaks:
    for l in chiamate_orarie.collect():
        print >>peaks, "%s,%s,%s,%s" % (l[0][0], l[0][1], l[0][2], l[1])
