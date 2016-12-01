import datetime
from pyspark import SparkContext,StorageLevel,RDD
from pyspark.serializers import MarshalSerializer
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
from sklearn.cluster import KMeans
import numpy as np
import hdfs
import time
import os,sys 
from cdr import CDR,Dataset
from collections import defaultdict

"""
User Profiling Module

Given a CDR dataset and a set of geographical regions, it returns user profiles for each spatial region.

Usage: user_profiling.py <folder> <spatial_division> <region> <timeframe> 

--folder: hdfs folder where the CDR dataset is placed
--spatial division: csv file with the format GSM tower id --> spatial region
--region,timeframe: file name desired for the stored results. E.g. Roma 11-2015

example: pyspark user_profiling.py dataset_simulated/06 ../spatial_regions/aree_roma.csv roma 06-2015

Results are stored into hdfs: /peaks/profiles-<region>-<timeframe>

"""


########################functions##################################
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)



def normalize(profilo):
##normalizza giorni chiamate su week end e  workday

	return [(x[0],x[1],x[2],x[3],x[4]*1.0/(2 if x[2]==1 else 5)) for x in profilo]

def check_complete_weeks_fast():
        """ week check with file names. 
        """
        weeks=defaultdict(list)
        for x in hdfs.ls("/"+"ttmetro/roma"):
            for f in hdfs.ls(x):
                try:
                    print f
                    d=datetime.datetime.strptime(f.split("_")[5].split(".")[0], "%Y%m%d")
                    weeks[(d.isocalendar()[0],d.isocalendar()[1])]+=[f]
                except:
                    pass
        return {x:f for x,f in weeks.iteritems() if len(f)>=5}
##########################################################################################

folder=sys.argv[1]
spatial_division=sys.argv[2]
region=sys.argv[3]
timeframe=sys.argv[4]



###spatial division: cell_id->region of interest

file=open(spatial_division)
#converting cell to municipality
cell2municipi={k:v for k,v in [(x.split(';')[0],x.split(';')[1].replace("\n","")) for x in file.readlines()]}

###data loading
#checking file existance
#####
sc=SparkContext()
quiet_logs(sc)
file_path='hdfs://hdp1.itc.unipi.it:9000/%s'%folder
files=[]
nfile=[]


start=time.time()
rddlist=[]



field2col={'user_id':1,'cdr_type':0,'start_cell':9,'end_cell':8,'date':3,'time':4}

##r= sc.textFile(','.join(files)).map(lambda row: CDR(row,field2col,'%Y%m%d',';')) \
##    .filter(lambda x: x.valid_region(cell2municipi)) \
##    .filter(lambda x: x.date!=None) \
##    .map(lambda x: ((x.user_id,x.region(cell2municipi),x.week_month(),x.is_we(),x.day_of_week(),x.day_time()),1)) \
##    .distinct() \
##    .map(lambda x: ((x[0][:4]+(x[0][5],)),1)) \
##    .reduceByKey(lambda x,y:x+y) \
##    .map(lambda x: (x[0][0],[x[0][1:]+(x[1],),])) \
##    .reduceByKey(lambda x,y:x+y)

#create a dataset of CDR
#dataset=Dataset( sc.textFile(','.join(files)).map(lambda row: CDR(row,field2col,'%Y%m%d',';')).filter(lambda x: x.date!=None).filter(lambda x: x.valid_region(cell2municipi))) 

###could it be useful to transform a dataset into an indexed version?
#dataset.date_filtering_static('rome_dataset_checked')
#for d in dataset.select_windows('01-01-2015',4): #start_date actually useless

#weeks_dict=check_complete_weeks_fast()
import cPickle as pk
weeks_dict=pk.load(open("weeks_dict.pkl","rb"))
#pk.dump(weeks_dict,open('weeks_dict.pkl','wb'))
weeks=sorted(weeks_dict.keys())

for i in range(0,len(weeks),2):
    files=[]

    for w in weeks[i:i+4]:
    	files+=weeks_dict[w]
    if '/ttmetro/roma/REPORT_ESTRAZIONE_TRAFF_COMUNE_ROMA_2016073.csv' not in files:
        continue
    dataset=Dataset( sc.textFile(','.join(files)).map(lambda row: CDR(row,field2col,'%Y%m%d',';')).filter(lambda x: x.date!=None).filter(lambda x: x.valid_region(cell2municipi))) 
    d=weeks[i]
    starting_week="%s_%s"%(d[0],d[1]) 
    r=dataset.data.map(lambda x: ((x.user_id,x.region(cell2municipi),x.week_month(weeks[i:i+4]),x.is_we(),x.day_of_week(),x.day_time(),x.week),1)) \
        .distinct() \
        .map(lambda x: ((x[0][:4]+(x[0][5],)),1)) \
        .reduceByKey(lambda x,y:x+y) \
        .map(lambda x: (x[0][0],[x[0][1:]+(x[1],),])) \
        .reduceByKey(lambda x,y:x+y)

    ###
    ### Carrello format: user -> [(municipio, settimana, weekend/workday, time_slice, count),...]
    ### nota: count= day of presence in the region at the timeslice




    ##week ordering
    ##keys: region,busiest week,workday/we,timeslice 
    r=r.map(lambda x: (x[0],sorted(x[1],key=lambda w: (w[0],sum([z[4] for z in x[1] if z[1]==w[1]]) ),reverse=True)))
   
    r=r.map(lambda x: (x[0],normalize(x[1]))) 
    
    #normalizzazione


    
    os.system("$HADOOP_HOME/bin/hadoop fs -rm -r /profiles/%s-%s/" %(region,starting_week))
    r.saveAsPickleFile('hdfs://hdp1.itc.unipi.it:9000/profiles/'+"%s-%s"%(region,starting_week))

print "elapsed time",time.time()-start
