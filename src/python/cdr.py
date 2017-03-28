#
# Copyright 2015-2017 ASAP
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

__author__ = 'paul'

import datetime
import os
import string

from pyspark import StorageLevel
from itertools import imap

from hdfs import Config

def check_complete_weeks_fast(path, filter_input, extract_date):
    files, weeks = explore_input(path, filter_input, extract_date)
    #if len(weeks) < 5:
    if len(weeks) < 4:
        raise Exception('No complete 4 weeks')
    return files, sorted(list(weeks))

def explore_input(path, filter_input, extract_date):
        """ week check with file names.
        """
        try:
            protocol, p = path.split('://')
            if protocol.lower() == 'hdfs':
                c = Config().get_client()
                files = filter(filter_input, c.list(p, status=True))
            elif protocol.lower() == 'file':
                files = [(f, {'pathSuffix': f}) for f in os.listdir(p)]
        except ValueError: # no protocol
            #TODO
            raise Exception('Unsupported no input protocol yet')
        else:
            t = map(lambda (_, d):
                    ('/'.join([path, d['pathSuffix']]),
                     extract_date(d['pathSuffix'])),
                    files)
            files, weeks = zip(*t)
            return files, set(weeks)

class CDR:

    def __init__(self, **kwargs):
        [setattr(self, k, v) for k, v in kwargs.items()]

    @classmethod
    def from_string(cls, row, field2col=None, dateformat='%Y-%m-%d %X', separator=';'):
        if field2col is None:
            field2col = {'user_id': 0,
                        'cdr_type': 11,
                        'start_cell': 9,
                        'end_cell': 10,
                        'date': 3,
                        'time': 4}
        fields = list(imap(string.strip, row.split(separator)))
        try:
            d = {k: fields[field2col[k]] for k in field2col}
            d['date'] = datetime.datetime.strptime(d['date'], dateformat)
            d['week'] = d['date'].isocalendar()[:2]
            return cls(**d)
        except:
            print('Invalid input: %s' % row)
            return

    def valid_region(self, cell2region):
        return self.start_cell in cell2region

    def region(self, cell2region):
        return cell2region[self.start_cell]

    def is_we(self):
        return 1 if self.date.weekday() in [0, 6] else 0

    def day_of_week(self):
        return self.date.weekday()

    def year(self):
        return self.date.year

    def day_time(self):
        # fascia oraria
        time = int(self.time.replace(" ", "")[:2])
        if time <= 8:
            return 0
        elif time <= 18:
            return 1
        else:
            return 2


class Dataset:
    """
    Definition of a CDR dataset, implementing time window functionalities
    """

    def __init__(self, rdd):
        self.data = rdd

    def check_complete_weeks(self):

        weeks = self.data.map(lambda x: (x.weeks, x.day_of_week())).distinct(
        ).groupByKey().map(lambda x: (x[0], len(list(x[1]))))
        complete_weeks = [x[0] for x in weeks.collect() if x[1] == 7]
        return complete_weeks

    def weeks_available(self):
        weeks = self.data.map(lambda x: (
            (x.date.isocalendar()[0], x.date.isocalendar()[1]))).distinct()
        return weeks.collect()

    def date_filtering_static(self, dir_name):
        """
        It check the available weeks and store the results into hdfs
        """
        weeks = sorted(self.check_complete_weeks_fast())
        for w in weeks:
            os.system("$HADOOP_HOME/bin/hadoop fs -rm -r /%s_%s-%s" %
                      (dir_name, w[0], w[1]))
            self.data.filter(lambda x: (x.date.isocalendar()[0], x.date.isocalendar()[1]) == w).saveAsPickleFile(
                'hdfs://hdp1.itc.unipi.it:9000/%s_%s-%s' % (dir_name, w[0], w[1]))

    def select_windows(self, start_date, window_shifts):
        """
        Input: starting date, string, e.g. "01-01-2015". Window shifts, int, e.g. 2
        Output: #window_shifts different datasets, shifted each 2 weeks
        """

        # filter dataset for complete weeks
        # weeks=sorted(self.check_complete_weeks())
        weeks = sorted(self.check_complete_weeks_fast())

        if len(weeks) < 4:
            print("there are less than 4 complete weeks in the dataset")
            # return

        self.data = self.data.filter(lambda x: x.weeks in weeks)
        for i in range(0, len(weeks), window_shifts):
            data_week = weeks[i:i + 4]
            if i > 0:
                self.data = self.data.filter(lambda x: x.weeks in weeks[i:]).persist(
                    storageLevel=StorageLevel(True, False, False, False, 1))
            yield data_week[0], self.data.filter(lambda x: (x.date.isocalendar()[0], x.date.isocalendar()[1]) in data_week).persist(storageLevel=StorageLevel(True, False, False, False, 1))


class Presence:

    def __init__(self, user_id, region, week_month, is_weekend, day_time):
        self.user_id = user_id
        self.region = region
        self.week_month = week_month
        self.is_weekend = is_weekend
        self.day_time = day_time
