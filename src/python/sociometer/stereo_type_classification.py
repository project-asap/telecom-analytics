import datetime
import os
import sys
from pyspark import SparkContext
from pyspark.mllib.clustering import KMeansModel

from dateutil import rrule
from utils import quiet_logs


"""Stereo Type Classification  Module

Given a set of user profiles and a set of labeled calling behaviors, it returns the percentage of each label
on each spatial region.
E.g.:
Region 1, resident, 75%
Region 1, commuter, 20%
...

Usage:
    $SPARK_HOME/bin/spark-submit sociometer/stereo_type_classification.py <profiles dataset> <centroids dataset> <start_date> <end_date>

Args:
    profile dataset: The profile dataset location. Can be any Hadoop-supported file system URI.
                     The expected dataset schema is:
                     <region>,<user_id>,<profile>.
                     The <profile> is a 24 element list containing the sum of user calls for each time division.
                     The column index for each division is: <week_idx> * 6 + <is_weekend> * 3 + <timeslot>
                     where <is_weekend> can be 0 or 1 and <timeslot> can be 0, 1, or 2.
    centroids dataset: The cluster dataset location. Can be any Hadoop-supported file system URI.
                       The expected dataset schema is:
                       <label>,<profile>.
                       The <profile> is a 24 element numpy array containing the cluster calls for each division.
                       The column index for each division is: <week_idx> * 6 + <is_weekend> * 3 + <timeslot>
                       where <is_weekend> can be 0 or 1 and <timeslot> can be 0, 1, or 2.
    start_date: The analysis starting date. Expected input %Y-%m-%d
    end_date: The analysis ending date. Expected input %Y-%m-%d

Results are stored into several local files: results/sociometer-<year>_<week_of_year>
where <year> and <week_of_year> are the year and week of year index of the starting week
of the 4 week analysis.

Example:
    $SPARK_HOME/bin/spark-submit \
        sociometer/stereo_type_classification.py hdfs:///profiles/roma hdfs:///centroids/roma \
        2016-01-01 2016-01-31

The results will be sotred in the local files:
results/sociometer-2015_53 etc
results/sociometer-2016_01 etc
results/sociometer-2016_02 etc
"""




def user_type(profile, model, centroids):
    if len([x for x in profile if x != 0]) == 1 and sum(profile) < 0.5:
        return 'passing by'
    else:
        idx = model.predict(profile)
        cluster = model.clusterCenters[idx]
        return centroids[cluster]


sc = SparkContext()
quiet_logs(sc)

ARG_DATE_FORMAT = '%Y-%m-%d'

profiles = sys.argv[1]
centroids = sys.argv[2]
start_date = datetime.datetime.strptime(sys.argv[3], ARG_DATE_FORMAT)
end_date = datetime.datetime.strptime(sys.argv[4], ARG_DATE_FORMAT)

weeks = [d.isocalendar()[:2] for d in rrule.rrule(
    rrule.WEEKLY, dtstart=start_date, until=end_date
)]

for year, week in weeks:
    subfolder = "%s/%s_%s" % (centroids, year, week)
    exists = os.system("$HADOOP_PREFIX/bin/hdfs dfs -test -e %s" % subfolder)
    if exists != 0:
        continue
    r = sc.pickleFile(subfolder)
    centroids = {tuple(v.tolist()): k for k, v in r.collect()}
    model = KMeansModel(centroids.keys())

    subfolder = "%s/%s_%s" % (profiles, year, week)
    r = sc.pickleFile(subfolder)

    r_auto = r.map(lambda (region, _, profile):  ((region, user_type(profile, model, centroids)), 1)) \
        .reduceByKey(lambda x, y: x + y)

    lst = r_auto.collect()
    sociometer = [(region,
                    user_type,
                    count * 1.0 / sum([c for ((r, u), c) in lst if r == region])
                    ) for ((region, user_type), count) in lst]

    with open("results/sociometer-%s_%s" % (year, week), 'w') as outfile:
        print >>outfile, "region, profile, percentage"
        for region, utype, count in sorted(sociometer, key=lambda x: x[0]):
            print >>outfile, region, utype, count
