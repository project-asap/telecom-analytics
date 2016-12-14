__author__ = 'paul'
import datetime
import string
import sys
import os
from itertools import imap
from pyspark import SparkContext, StorageLevel

from cdr import CDR
#from utils import quiet_logs

"""Data Filter Module.
Given a CDR dataset and a set of geographical regions, it returns the hourly
presence for each region. More specifically the results are tuples containing
the following information:
<region>,<day_of_week>,<hour_of_day>,<date>,<count>V

Usage:
    $SPARK_HOME/bin/spark-submit --py-files cdr.py peak_detection/data_filter.py \
    <dataset> <spatial_division> <region> <start_date> <end_date>

Args:
    dataset: The dataset location. Can be any Hadoop-supported file system URI.
             The expected dataset schema is:
             user_id;null;null;start_date;start_time;duration;null;null;null;start_gsm_cell;end_gsm_cell;record_type
             The start_time column is expected to have this format: '%Y-%m-%d %X'.
    spatial_division: File containing the mapping of cells to regions.
    region: The region name featuring in the stored results
    start_date: The starting date of the analysis (format: %Y-%m-%d)
    end_date: The ending date of the analysis (format: %Y-%m-%d)

The results are stored into the hdfs file: /peaks/hourly_presence/<region>/<start_date>_<end_date>

Example:
    $SPARK_HOME/bin/spark-submit --py-files cdr.py peak_detection/data_filter.py \
    hdfs:///dataset_simulated/2016 spatial_regions/aree_roma.csv roma 2016-01-01 2016-01-31

The results will be saved in: /peaks/hourly_presence/roma/2016-01-01_2016-01-31
"""

ARG_DATE_FORMAT = '%Y-%m-%d'

##########################################################################
folder = sys.argv[1]
spatial_division = sys.argv[2]
region = sys.argv[3]
start_date = datetime.datetime.strptime(sys.argv[4], ARG_DATE_FORMAT)
end_date = datetime.datetime.strptime(sys.argv[5], ARG_DATE_FORMAT)

# spatial division: cell_id->region of interest

file = open(spatial_division)
# converting cell to municipality
cell2region = {k: v for k, v in [
    imap(string.strip, x.split(';')) for x in file.readlines()]}

#####
sc = SparkContext()
#quiet_logs(sc)
#sc._conf.set(
#    'spark.executor.memory',
#    '24g').set(
#        'spark.driver.memory',
#        '24g').set(
#            'spark.driver.maxResultsSize',
#    '0')

# TODO check for complete weeks in dataset

r = sc.textFile(folder) \
    .map(lambda x: CDR.from_string(x)) \
    .filter(lambda x: x is not None) \
    .filter(lambda x: x.valid_region(cell2region)) \
    .filter(lambda x: start_date <= x.date <= end_date) \
    .map(lambda x: (x.user_id,
                     x.region(cell2region),
                     x.day_of_week(),
                     x.time[:2],
                     x.date)) \
    .distinct() \
    .persist(StorageLevel(True, False, False, False, 1))


#
chiamate_orarie = r.map(
    lambda (_, region, dow, hour, date): (
        (region, dow, hour, date), 1.0)).reduceByKey( lambda x, y: ( x + y))


output = '/peaks/hourly_presence/%s/%s_%s' % (
    region, start_date.strftime(ARG_DATE_FORMAT), end_date.strftime(ARG_DATE_FORMAT))
os.system("$HADOOP_HOME/bin/hadoop fs -rm -r %s" % output)
chiamate_orarie.saveAsPickleFile(output)
