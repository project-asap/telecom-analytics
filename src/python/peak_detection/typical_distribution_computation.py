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

"""Typical Distribution Computation Module.

Given an hourly presence dataset (usually regarding a month of activity),
it aggregates the presences ccording to week days and hours.
More specifically the results are tuples containing the following information:
<region>,<day_of_week>,<hour_of_day>,<count>

Usage:
    $SPARK_HOME/bin/spark-submit peak_detection/typical_distribution_computation.py <dataset>

Args:
    dataset: The hourly presence dataset consisting of tuples containing the
             following information:
             <region>,<day_of_week>,<hour_of_day>,<date>,<count>

Results are stored into hdfs: /peaks/weekly_<region>_<start_date>_<end_date>
where the <region>, <start_date> and <end_date> are derived by the name of the dataset.

Example:
    $SPARK_HOME/bin/spark-submit peaks_detection/typical_distribution_computation.py \
/peaks/hourly_roma_2016-01-01_2016-01-31

"""

__author__ = 'paul'

from pyspark import SparkContext

import os
import sys



########################functions##################################
if __name__ == '__main__':
    folder = sys.argv[1]

    sc = SparkContext()

    hourly_calls = sc.pickleFile(folder)

    # format: region, weekday, hour
    mean_presence = hourly_calls.map(
        lambda ((region, dow, hour, start_date), count): (
            (region, dow, hour), count)
    ).groupByKey()

    name = folder.replace('hourly', 'weekly')
    os.system("$HADOOP_PREFIX/bin/hadoop fs -rm -r %s" % name)
    mean_presence.saveAsPickleFile(name)
