__author__ = 'paul'
import datetime
import os
from collections import defaultdict
import hdfs
from pyspark import SparkContext,StorageLevel,RDD
###CDR class

def check_complete_weeks_fast(path):
        """ week check with file names. 
        """
        weeks=defaultdict(list)
        for x in hdfs.ls("/"+path):
            for f in hdfs.ls(x):
                try:
                    print f
                    d=datetime.datetime.strptime(f.strip("/"+path).split("_")[5].split(".")[0], "%Y%m%d")
                    weeks[(d.isocalendar()[0],d.isocalendar()[1])]+=[f]
                except:
                    raise
                    pass
        return {x:f for x,f in weeks.iteritems() if len(f)>=5}

class CDR:


    def __init__(self,row,field2col,dateformat,separator):
        try:
            row=row.split(separator)
            self.user_id=row[field2col['user_id']]
            self.cdr_type=row[field2col['cdr_type']]
            self.start_cell=row[field2col['start_cell']]
            self.end_cell=row[field2col['end_cell']]
            try:
                self.date=datetime.datetime.strptime(row[field2col['date']], dateformat)
                self.date=datetime.date(self.date.year, self.date.month, self.date.day)
                self.week=(self.date.isocalendar()[0],self.date.isocalendar()[1])
            except ValueError:
                self.date=None
            self.time=row[field2col['time']]
        except KeyError:
            raise Exception('field2col not properly defined')
    def valid_region(self,cell2region):
        try:
            c=cell2region[self.start_cell]
            return True
        except KeyError:
            return False
    def region(self,cell2region):

        return cell2region[self.start_cell]

    def is_we(self):
        return 1 if self.date.weekday() in [0,6] else 0
    def day_of_week(self):
        return self.date.weekday()
    def year(self):
        return self.date.year
    def week_month(self,weeks):
        #settimana del mese
        #year,week=self.date.isocalendar()[0],self.date.isocalendar()[1]
        return weeks.index((self.week))
    def day_time(self):
    #fascia oraria
        time=int(self.time.replace(" ","")[:2])
        if time<=8:
            return 0
        elif time<=18:
            return 1
        else:
            return 2

class Dataset:
    """
    Definition of a CDR dataset, implementing time window functionalities
    """
    def __init__(self,rdd):
        self.data=rdd
    
    def check_complete_weeks(self):

        weeks=self.data.map(lambda x: (x.weeks,x.day_of_week())).distinct().groupByKey().map(lambda x: (x[0],len(list(x[1]))))
        complete_weeks=[x[0] for x in weeks.collect() if x[1]==7]
    	return complete_weeks
    def weeks_available(self):
        weeks=self.data.map(lambda x: ((x.date.isocalendar()[0],x.date.isocalendar()[1]))).distinct()
        return weeks.collect()
    def date_filtering_static(self,dir_name):
        """
        It check the available weeks and store the results into hdfs   
        """
        weeks=sorted(self.check_complete_weeks_fast())
        for w in weeks:
            os.system("$HADOOP_HOME/bin/hadoop fs -rm -r /%s_%s-%s"%(dir_name,w[0],w[1]))
            self.data.filter(lambda x: (x.date.isocalendar()[0],x.date.isocalendar()[1]) ==w).saveAsPickleFile('hdfs://hdp1.itc.unipi.it:9000/%s_%s-%s'%(dir_name,w[0],w[1]))
        
    def select_windows(self,start_date,window_shifts):
        """
        Input: starting date, string, e.g. "01-01-2015". Window shifts, int, e.g. 2
        Output: #window_shifts different datasets, shifted each 2 weeks
        """
        
        #filter dataset for complete weeks
        #weeks=sorted(self.check_complete_weeks())
        weeks=sorted(self.check_complete_weeks_fast())

        if len(weeks)<4:
            print "there are less than 4 complete weeks in the dataset"
            #return
        
        self.data=self.data.filter(lambda x: x.weeks in weeks)
        for i in range(0,len(weeks),window_shifts):
            data_week=weeks[i:i+4]
            if i>0:
                self.data=self.data.filter(lambda x: x.weeks in weeks[i:]).persist(storageLevel=StorageLevel(True, False, False, False, 1))
            yield data_week[0],self.data.filter(lambda x: (x.date.isocalendar()[0],x.date.isocalendar()[1])  in data_week).persist(storageLevel=StorageLevel(True, False, False, False, 1))
        
        
        

class Presence:
    def __init__(self,user_id,region,week_month,is_weekend,day_time):
        self.user_id=user_id
        self.region=region
        self.week_month=week_month
        self.is_weekend=is_week_end
        self.day_time=day_time
