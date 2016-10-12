#
# Copyright 2015-2016 WIND,FORTH
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import datetime
from pyspark import SparkContext, StorageLevel
import time
import os
import sys
import string as str_

from itertools import imap

#DATE_FORMAT = '%Y%m%d'
DATE_FORMAT = '%Y-%m-%d'

MIN_PARTITIONS = 512

#weeks = [47,48,49,50]
weeks = [27, 28, 29, 30]

"""
User Profiling Module

Given a CDR dataset and a set of geographical regions, it returns user profiles for each spatial region.

Usage: user_profiling.py <folder> <spatial_division> <region> <timeframe>

--folder: hdfs folder where the CDR dataset is placed
--spatial division: csv file with the format GSM tower id --> spatial region
--region,timeframe: file name desired for the stored results. E.g. Roma 11-2015

example: pyspark user_profiling.py /dataset_simulated/06 ../spatial_regions/aree_roma.csv roma 06-215

Results are stored into hdfs: /profiles-<region>-<timeframe>

"""


########################functions##################################
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def euclidean(v1, v2):
    print v1, v2
    return sum([abs(v1[i] - v2[i])**2 for i in range(len(v1))])**0.5


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i + n]


def validate(date_text):
    # if the string is a date, return True (useful to filter csv header)
    try:
        datetime.datetime.strptime(date_text, DATE_FORMAT)
        return True
    except ValueError:
        return False


def week_month(string):
    # settimana del mese
    d = datetime.datetime.strptime(string, DATE_FORMAT)
    w = (d.day - 1) // 7 + 1
    return w


def filter_week_higher(week):
    if week >= 4:
        return False
    else:
        return True


def is_we(string):
    d = datetime.datetime.strptime(string, DATE_FORMAT)
    return 1 if d.weekday() in [0, 6] else 0


def day_of_week(string):
    d = datetime.datetime.strptime(string, DATE_FORMAT)
    return d.weekday()


def day_time(string):
    # fascia oraria
    time = int(string[:2])
    if time <= 8:
        return 0
    elif time <= 18:
        return 1
    else:
        return 2


def annota_utente(profilo, centroids, profiles):
    for munic in set([x[0] for x in profilo]):
        # settimana,work/we,timeslice, count normalizzato
        obs = [x[1:] for x in profilo if x[0] == munic]
        # carr=np.zeros(24)
        carr = [0 for x in range(18)]
        for o in obs:
            idx = (o[0] - 1) * 6 + o[1] * 3 + o[2]
            carr[idx] = o[3]
        # returns the index of the closest centroid
        tipo_utente = sorted([(i, euclidean(carr, c)) for i, c in enumerate(
            centroids)], key=lambda x: x[1])[0][0]
        yield (munic, profiles[tipo_utente])


def normalize(profilo):
    # normalizza giorni chiamate su week end e  workday

    return [(x[0], x[1], x[2], x[3], x[4] * 1.0 / (2 if x[2] == 1 else 5)) for x in profilo]


def municipio(cell_id):
    try:
        cell2municipi[cell_id]
        return True
    except KeyError:
        return False


def cavallo_week(string):
    d = datetime.datetime.strptime(string, DATE_FORMAT)
    return d.isocalendar()[1] in weeks
##########################################################################

folder = sys.argv[1]
spatial_division = sys.argv[2]
region = sys.argv[3]
timeframe = sys.argv[4]

# spatial division: cell_id->region of interest

with open(spatial_division) as file:
    # converting cell to municipality
    cell2municipi = {k: v for k, v in [(x.split(';')[0], x.split(
        ';')[1].replace("\n", "")) for x in file.readlines()]}


# data loading
# checking file existance
#####
sc = SparkContext()
quiet_logs(sc)

start = time.time()

# Processo:
# count giorni distinti di chiamata per ogni timeslot
# rimuovo day of week -> indicatore se ha telefonato o no in quel giorno in quello slot
# sommo giorni di chiamata per ogni slot
lines = sc.textFile(folder, minPartitions=MIN_PARTITIONS).map(
    lambda l: list(imap(str_.strip, l.split(';'))))
#lines = sc.textFile(folder).map(lambda l: list(imap(string.strip, l.split(';')))).cache

# if lines.count() % 7 != 0:
#    print "missing complete weeks in dataset"
#    #exit()

r = lines.filter(lambda x: municipio(x[9])) \
    .filter(lambda x: validate(x[3])) \
    .filter (lambda x: cavallo_week(x[3])) \
    .map(lambda x: ((x[1], cell2municipi[x[9]] , week_month(x[3]),  is_we(x[3]) , day_of_week(x[3]) , day_time(x[4])), 1)) \
    .distinct() \
    .map(lambda x: ((x[0][:4] + (x[0][5],)), 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .map(lambda x: (x[0][0], [x[0][1:] + (x[1],), ])) \
    .reduceByKey(lambda x, y: x + y) \
    .persist(StorageLevel(False, True, False, False))

###
# Carrello format: user -> [(municipio, settimana, weekend/workday, time_slice, count),...]
# nota: count= day of presence in the region at the timeslice

# week ordering
# keys: region,busiest week,workday/we,timeslice
r = r.map(lambda x: (x[0], sorted(x[1], key=lambda w: (
    w[0], sum([z[4] for z in x[1] if z[1] == w[1]]), -w[2], w[3]), reverse=True)))

r = r.map(lambda x: (x[0], normalize(x[1])))

# normalizzazione

print r.count()

os.system("$HADOOP_HOME/bin/hadoop fs -rm -r /profiles-%s-%s/" %
          (region, timeframe))
r.saveAsPickleFile('/profiles-%s-%s' % (region, timeframe))
