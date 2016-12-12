__author__ = 'paul'
import string
import sys

from itertools import imap
from sets import Set

from cdr import CDR
from utils import quiet_logs

from pyspark import SparkContext, StorageLevel

"""Spatio-temporal Aggregation module.

Given a CDR dataset and a set of geographical regions, it returns presence timeseries for each spatial region.
More specifically the results are tuples containing the following information:
<region>,<date>,<time>,<count>,<start-cells>

Usage:
    $SPARK_HOME/bin/spark-submit --py-files cdr.py,utils.py statistics/spatio_temporal_aggregation.py \
<dataset> <spatial_division> <region> <timeframe>

Args:
    dataset:The dataset location. Can be any Hadoop-supported file system URI.
            The expected dataset schema is:
            user_id;null;null;start_date;start_time;duration;null;null;null;start_gsm_cell;end_gsm_cell;record_type
            The start_time column is expected to have this format: '%Y-%m-%d %X'.
    spatial_division: File containing the mapping of cells to regions.
    region: The region name featuring in the stored results
    timeframe: The timeframe featuring in the stored results

Results are stored into the local file: timeseries-<region>-<timeframe>.

Example:
    $SPARK_HOME/bin/spark-submit --py-files cdr.py,utils.py statistics/spatio_temporal_aggregation.py \
hdfs:///dataset_simulated/2016 spatial_regions/aree_roma.csv roma 2016
"""

def region_coordinations(region, cell2region, cell2coordinates):
    points = [cell2coordinates(k) for k in cell2region if cell2region[k] == region]

folder = sys.argv[1]
spatial_division = sys.argv[2]
region = sys.argv[3]
timeframe = sys.argv[4]

# spatial division: cell_id->region of interest
with open(spatial_division) as f:
    # converting cell to municipality
    cell2region = {k: v for k, v in [
        imap(string.strip, x.split(';')) for x in f.readlines()]}

sc = SparkContext()
quiet_logs(sc)

peaks = open('timeseries-%s-%s' % (region, timeframe), 'w')

lines = sc.textFile(folder) \
    .map(lambda x: CDR.from_string(x)) \
    .filter(lambda x: x is not None) \
    .filter(lambda x: x.valid_region(cell2region)) \
    .map(lambda x: (x.user_id, x.start_cell, x.region(cell2region), x.date, x.time[:2] )) \
    .distinct() \
    .persist(StorageLevel(False, True, False, False))

chiamate_orarie = lines.keyBy(lambda x: x[2:])\
    .map(lambda (k, (_1, cell, _2, _3, _4)): (k, (1, Set([cell]))))\
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] | y[1]))

for ((region, date, time), (count, cells)) in chiamate_orarie.collect():
    print >>peaks, "%s,%s,%s,%s,%s" % (region, date, time, count, cells)
