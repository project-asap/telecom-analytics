__author__ = 'paul'
import datetime
from pyspark import SparkContext

import string as str_

from itertools import imap

#date_format = '%Y%m%d'
date_format = '%Y-%m-%d'

"""
Data Filter Module

Given a CDR dataset and a set of geographical regions, it returns the hourly presence for each region.

Usage: data_filter.py <folder> <spatial_division> <region> <timeframe>

--folder: hdfs folder where the CDR dataset is placed
--spatial division: csv file with the format GSM tower id --> spatial region
--region,timeframe: file name desired for the stored results. E.g. Roma 11-2015

example: pyspark data_filter.py /dataset_simulated/06 ../spatial_regions/aree_roma.csv roma 06-215

Results are stored into hdfs: /peaks/hourly_presence-<region>-<timeframe>

"""


import time
import os
import sys
########################functions##################################


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def validate(date_text):
    # if the string is a date, return True (useful to filter csv header)
    try:
        datetime.datetime.strptime(date_text, date_format)
        return True
    except ValueError:
        return False


def week_month(string):
    # settimana del mese
    d = datetime.datetime.strptime(string, date_format)
    return d.isocalendar()[1]


def day_of_week(string):
    d = datetime.datetime.strptime(string, date_format)
    return d.weekday()


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
sc._conf.set('spark.executor.memory', '24g').set(
    'spark.driver.memory', '24g').set('spark.driver.maxResultsSize', '0')

start = time.time()
lines = sc.textFile(folder, minPartitions=32).map(
    lambda l: list(imap(str_.strip, l.split(';'))))
#if len(files) % 7 != 0:
#    print "missing complete weeks in dataset"
#    # exit()

# plot utenti con chiamate durante partite
r = lines.filter(lambda x: validate(x[3])) \
    .filter(lambda x: municipio(x[9])) \
    .map(lambda x: ((x[1], cell2municipi[x[9]], day_of_week(x[3]), week_month(x[3]), x[4][:2], x[3]), 1)) \
    .distinct() \
    .reduceByKey(lambda x, y: x + y)

###

chiamate_orarie = r.map(lambda x: ((x[0][1], x[0][2], x[0][3], x[0][4], x[
                        0][5]), 1.0)).reduceByKey(lambda x, y: (x + y))


os.system("$HADOOP_HOME/bin/hadoop fs -rm -r /peaks/hourly_presence-%s-%s/" %
          (region, timeframe))
chiamate_orarie.saveAsPickleFile(
    '/peaks/hourly_presence-' + "%s-%s" % (region, timeframe))
