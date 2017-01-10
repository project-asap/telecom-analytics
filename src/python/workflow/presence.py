import datetime
from pyspark import SparkContext,StorageLevel,RDD
from pyspark.serializers import MarshalSerializer
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
from sklearn.cluster import KMeans
import numpy as np
import time
import os,sys 
from cdr import CDR,Dataset
from collections import defaultdict
import cdr

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)




def get_key_str(date,granularity):
    """
        :param date datetime: 
        :param granularity text: <day> ritorna una chiave col formato (yyyy,mm,dd)
                                 <day-hour> ritorna una chiave col formato (yyyy,mm,dd,hh)
    """
    if granularity=='day':
        return date[:4],date[4:6],date[6:8]
    elif granularity=='day-hour':
        return date[:4],date[4:6],date[6:8],date[8:10]
# ## Lettura dell'elenco delle antenne associate alle zone



interest_region,user2label,field2col,folder=sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4]



#weeks_dict=cdr.check_complete_weeks_fast(folder)
import cPickle as pk
weeks_dict=pk.load(open("weeks_dict.pkl","rb"))
files=[]

for w in weeks_dict:
    files+=weeks_dict[w]
from pyspark.serializers import BatchedSerializer, PickleSerializer
# sc = SparkContext(
#     "local", "bar",
#     serializer=PickleSerializer(),  # Default serializer
#     # Unlimited batch size -> BatchedSerializer instead of AutoBatchedSerializer
#     batchSize=-1  
# )
sc=SparkContext("spark://131.114.136.218:7077")

quiet_logs(sc)
sc._conf.set('spark.executor.memory','32g').set('spark.driver.memory','32g').set('spark.driver.maxResultsSize','0')

field2col={x.split(",")[0]:int(x.split(",")[1]) for x in open(field2col).readlines()}
f=open(user2label) 
user2label={}
i=0
for row in f:
    d=row.split(";")
    user,region,labels=d

    if user not in user2label:
        user2label[user]={}
    user2label[user][region]=labels.strip("\n")
    i+=1
    if i%1000000==0:
        print i, "users added"

user_rdd=sc.broadcast(user2label)

sites2zones={}
f=open(interest_region)
for row in f:
    row=row.strip().split(';')
    sites2zones[row[0][:5]]=row[1]
zone_rdd=sc.broadcast(sites2zones)
def user_join(region,user):
    if user in user_rdd.value[region]:
        return user_rdd.value[region][user]
    return "not classified"

def zone_join(region):
    if region in sites2zones:
        return sites2zones[region]
    return "out"
results=[]
lst=sites2zones.keys()
from itertools import islice
for f in files:
	start_time=time.time()
	date=f.strip("/"+folder).split("_")[5][:8].strip(".")
	dataset=sc.textFile(f).filter(lambda x: x.split(";")[field2col['start_cell']][:5] in zone_rdd.value and x.split(";")[field2col['user_id']] in user_rdd.value)

	dataset=dataset.map(lambda x: (x.split(";")[field2col['user_id']],zone_rdd.value[x.split(";")[field2col['start_cell']][:5]])).distinct()
	dataset=dataset.map(lambda x: ((user_join(x[0],x[1]),x[1],date),1)).reduceByKey(lambda x,y:x+y)
	#os.system("$HADOOP_HOME/bin/hadoop fs -rm -r /presence_timeseries/%s"%date)
	#dataset.saveAsTextFile("hdfs://hdp1.itc.unipi.it:9000/presence_timeseries/%s"%date)
	results+=dataset.collect()

out_file=open("results/presence_timeseries-%.csv"%folder,"w")
for r in results:
    print >> out_file, "%s;%s;%s;%s"%(r[0][1],r[0][2],r[0][0],r[1])




