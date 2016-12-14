__author__ = 'paul'
from pyspark import SparkContext
import numpy as np

import sys

"""Peak detection Module.

Given an hourly presence dataset (usually regarding a month of activity),
and a typical weekly presence dataset, it computes the relative presences
for each hour of the month, in order to identify eventual peaks of presences.

Usage:
    $SPARK_HOME/bin/spark-submit peak_detection/peak_detection.py \
        <hourly_dataset> <weekly_dataset> <region> <start_date> <end_date>

Args:
    hourly_dataset: The hourly presence dataset prefix. Can be any Hadoop-supported URI.
                    The full path dataset name it computed as:
                    <folder>/<region>/<start_date>_end_date>
                    The dataset consists of tuples containing the following information:
                    <region>,<day_of_week>,<hour_of_day>,<date>,<count>
    weekly_dataset: The hourly presence dataset prefix. Can be any Hadoop-supported URI.
                    The full path dataset name it computed as:
                    <folder>/<region>/<start_date>_end_date>
                    The dataset consists of tuples containing the following information:
                    <region>,<day_of_week>,<hour_of_day>,<count>
    region: The region name featuring in dataset full path name and in the stored results
    start_date: The starting date of the analysis (format: %Y-%m-%d)
    end_date: The ending date of the analysis (format: %Y-%m-%d)


Results are stored into the local file: peaks_<region>_<start_date>_<end_date>.

Example:
$SPARK_HOME/bin/spark-submit peaks_detection/peak_detection.py \
    /peaks/hourly_presence /peaks/weekly_presence roma 2016-01-01 2016-01-31

The results will be saved in the local file: peaks_roma_2016-01-01_2016-01-31
"""

ARG_DATE_FORMAT = '%Y-%m-%d'

hourly_dataset = sys.argv[1]
weekly_dataset = sys.argv[2]
region = sys.argv[3]
start_date = sys.argv[4]
end_date = sys.argv[5]

sc = SparkContext()

weekly_presence = sc.pickleFile('%s/%s/%s_%s' % (weekly_dataset, region, start_date, end_date)) \
    .collectAsMap()

hourly_presence = sc.pickleFile('%s/%s/%s_%s' % (hourly_dataset, region, start_date, end_date))

peaks = open( 'peaks_%s_%s_%s.csv' % (region, start_date, end_date), 'w')
# format: area,hour,date->percentage
for ((region, dow, hour, start_date), count) in hourly_presence.collect():
    print >>peaks, "%s,%s,%s,%s" % (region, start_date, hour,
                                    count / np.mean(list(weekly_presence[(region, dow, hour)])))
