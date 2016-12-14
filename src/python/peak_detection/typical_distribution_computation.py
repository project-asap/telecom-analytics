__author__ = 'paul'
from pyspark import SparkContext

import os
import sys


"""Typical Distribution Computation Module.

Given an hourly presence dataset (usually regarding a month of activity),
it aggregates the presences according to week days and hours.
More specifically the results are tuples containing the following information:
<region>,<day_of_week>,<hour_of_day>,<count>

Usage:
    $SPARK_HOME/bin/spark-submit peak_detection/typical_distribution_computation.py \
        <folder> <region> <start_date> <end_date>

Args:
    folder: The hourly presence dataset prefix. Can be any Hadoop-supported URI.
    The full path dataset name it computed as:
    <folder>/<region>/<start_date>_end_date>
    The dataset consists of tuples containing the following information:
    <region>,<day_of_week>,<hour_of_day>,<date>,<count>
    region: The region name featuring in dataset full path name and in the stored results
    start_date: The starting date of the analysis (format: %Y-%m-%d)
    end_date: The ending date of the analysis (format: %Y-%m-%d)

Results are stored into hdfs: /peaks/weekly_<region>_<start_date>_<end_date>
where the <region>, <start_date> and <end_date> are derived by the name of the dataset.

Example:
$SPARK_HOME/bin/spark-submit peak_detection/typical_distribution_computation.py \
    /peaks/hourly_presence roma 2016-01-01 2016-01-31

The results will be saved in: /peaks/hourly/roma/2016-01-01_2016-01-31
"""

folder = sys.argv[1]
region = sys.argv[2]
start_date = sys.argv[3]
end_date = sys.argv[4]

sc = SparkContext()

hourly_presence = sc.pickleFile('%s/%s/%s_%s' % (folder, region, start_date, end_date))

# format: area,weekday,hour
weekly_presence = hourly_presence.map(lambda (t, count): (
    t[:-1], count)).groupByKey()

output = '/peaks/weekly_presence/%s/%s_%s' % (region, start_date, end_date)
os.system("$HADOOP_HOME/bin/hadoop fs -rm -r %s" % output)
weekly_presence.saveAsPickleFile(output)
