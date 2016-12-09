__author__ = 'paul'
import string
import sys

from itertools import imap
from cdr import CDR
from utils import quiet_logs

from pyspark import SparkContext, StorageLevel

"""Spatio-temporal Aggregation module.

Given a CDR dataset and a set of geographical regions, it returns presences timeseries for each spatial region.
More specifically the results are tuples containing the following information:
<region>,<date>,<time>,<count>

Usage:
    $SPARK_HOME/bin/spark-submit --py-files cdr.py statistics/spatio_temporal_aggreation.py \
<dataset> <spatial_division> <region> <timeframe>

Args:
    dataset:The dataset location. Can be any Hadoop-supported file system URI.
            The expected dataset schema is:
            user_id;null;null;start_date;start_time;duration;null;null;null;start_gsm_cell;end_gsm_cell;record_type
            The start_time column is expected to have this format: '%Y-%m-%d %X'.
    spatial_division: File containing the mapping of cells to regions.
    region: The region name featuring in the stored results
    timeframe: The timeframe featuring in the stored results

Results are stored into the local file: timeseries-<region>-<timeframe>
where the <region> is derived by the spatial_division.

Example:
    $SPARK_HOME/bin/spark-submit --py-files cdr.py statistics/spatio_temporal_aggreation.py \
hdfs:///dataset_simulated/2016 spatial_regions/aree_roma.csv roma 2016
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

peaks = open('timeseries-%s-%s' % (region, timeframe), 'w')

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
