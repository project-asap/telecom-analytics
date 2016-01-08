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
########################functions##################################

def euclidean(v1,v2):
	print v1,v2
	return sum([abs(v1[i]-v2[i])**2 for i in range(len(v1))])**0.5

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i+n]
def validate(date_text):
### if the string is a date, return True (useful to filter csv header)
    try:
        datetime.datetime.strptime(date_text, '%Y%m%d')
        return True
    except ValueError:
        return False
def week_month(string):
    #settimana del mese
    d=datetime.datetime.strptime(string, '%Y%m%d')
    w=(d.day-1)//7+1
    return d.isocalendar()[1]

def is_we(string):
    d=datetime.datetime.strptime(string, '%Y%m%d')
    return 1 if d.weekday() in [0,6] else 0
def day_of_week(string):
    d=datetime.datetime.strptime(string, '%Y%m%d')
    return d.weekday()
def day_time(string):
    #fascia oraria
    time=int(string[:2])
    if time<=8:
    	return 0
    elif time<=18:
    	return 1
    else:
    	return 2

def annota_utente(profilo,centroids,profiles):
	for munic in set([x[0] for x in profilo]):
		##settimana,work/we,timeslice, count normalizzato
		obs=[x[1:] for x in profilo if x[0]==munic]
		#carr=np.zeros(24)
		carr=[0 for x in range(24)]
		for o in obs:
			idx=(o[0]-1)*6+o[1]*3+o[2]
			carr[idx]=o[3]
		##returns the index of the closest centroid
		tipo_utente=sorted([(i,euclidean(carr,c)) for i,c in enumerate(centroids)],key=lambda x:x[1])[0][0]
		yield (munic,profiles[tipo_utente])





def normalize(profilo):
##normalizza giorni chiamate su week end e  workday

	return [(x[0],x[1],x[2],x[3],x[4]*1.0/(2 if x[2]==1 else 5)) for x in profilo]

def municipio(cell_id):
	try:
		c=cell2municipi[cell_id]
		return True
	except KeyError:
		return False


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

file_path='hdfs://hdp1.itc.unipi.it:9000/%s'%folder
files=[]
nfile=[]

for x in hdfs.ls("/"+folder):
	files.append("hdfs://hdp1.itc.unipi.it:9000%s"%(x))


start=time.time()
rddlist=[]
if len(files)%7!=0:
    print "missing complete weeks in dataset"
    exit()

### split into more RDD, otherwise it dies
for i in range(0,len(files),7):
    print "week n.",i
    loc_file=files[i:i+7]
    
###Processo:
 ##count giorni distinti di chiamata per ogni timeslot
## rimuovo day of week -> indicatore se ha telefonato o no in quel giorno in quello slot
## sommo giorni di chiamata per ogni slot

    lines= sc.textFile(','.join(loc_file)) \
      .filter(lambda x: municipio(x.split(';')[9])) \
      .map(lambda x: ((x.split(';')[1], cell2municipi[x.split(';')[9]] , week_month(x.split(';')[3]),  is_we(x.split(';')[3]) , day_of_week(x.split(';')[3]) , day_time(x.split(';')[4])),1)) \
      .distinct() \
      .map(lambda x: ((x[0][:4]+(x[0][5],)),1)) \
      .reduceByKey(lambda x,y:x+y) \
      .map(lambda x: (x[0][0],[x[0][1:]+(x[1],),])) \
      .reduceByKey(lambda x,y:x+y) \
      .persist( StorageLevel(False, True, False, False)) 
    
    rddlist.append(lines)
for r in range(len(rddlist)-1):
    rddlist[r+1]=rddlist[r].union(rddlist[r+1])
    
###
### Carrello format: user -> [(municipio, settimana, weekend/workday, time_slice, count),...]
### nota: count= day of presence in the region at the timeslice

### risultato dei join dei veri RDD
r=rddlist[-1].reduceByKey(lambda x,y:x+y)


##week ordering
##keys: region,busiest week,workday/we,timeslice 
r=r.map(lambda x: (x[0],sorted(x[1],key=lambda w: (w[0],sum([z[4] for z in x[1] if z[1]==w[1]]) , -w[2],w[3]),reverse=True)))

r=r.map(lambda x: (x[0],normalize(x[1]))) 

#normalizzazione




os.system("$HADOOP_HOME/bin/hadoop fs -rm -r /profiles/%s-%s" %(region,timeframe))
r.saveAsPickleFile('hdfs://hdp1.itc.unipi.it:9000/profiles/'+"%s-%s"%(region,timeframe))