__author__ = 'paul'
import datetime
from pyspark import SparkContext,StorageLevel
import hdfs

import time
import sys

from urlparse import urljoin

"""
Spatio-temporal Aggregation module

Given a CDR dataset and a set of geographical regions, it returns presences timeseries for each spatial region.

Usage: spatio_temporal_aggreation.py <folder> <spatial_division> <region> <timeframe>

--folder: hdfs folder where the CDR dataset is placed
--spatial division: csv file with the format GSM tower id --> spatial region
--region,timeframe: file name desired for the stored results. E.g. Roma 11-2015

example: pyspark spatio_temporal_aggreation.py dataset_simulated/06 ../spatial_regions/aree_roma.csv roma 06-2015

Results are stored into file: timeseries-<region>-<timeframe>-<spatial_division>.csv

"""


########################functions##################################
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
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
        datetime.datetime.strptime(date_text, ' %Y-%m-%d ')
        return True
    except ValueError:
        return False
def week_month(string):
    #settimana del mese
    d=datetime.datetime.strptime(string, ' %Y-%m-%d ')
    return d.isocalendar()[1]

def is_we(string):
    d=datetime.datetime.strptime(string, ' %Y-%m-%d ')
    return 1 if d.weekday() in [0,6] else 0
def day_of_week(string):
    d=datetime.datetime.strptime(string, ' %Y-%m-%d ')
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
		cell2municipi[cell_id]
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
cell2municipi={k:v for k,v in [(x.split(';')[0].replace(" ",""),x.split(';')[1].replace("\n","")) for x in file.readlines()]}

HDFS_BASE = 'hdfs://hdp1.itc.unipi.it:9000/'

###data loading
#checking file existance
#####
sc=SparkContext()
quiet_logs(sc)
file_path = urljoin(HDFS_BASE, folder)
print file_path
files=[]
nfile=[]
for x in hdfs.ls(folder)[:]:
    if "BARBERINO" in x:
        print x
        continue
    files.append(urljoin(HDFS_BASE, x))
start=time.time()
rddlist=[]

peaks=open('timeseries%s-%s-%s.csv'%(region,timeframe,spatial_division.split("/")[-1]),'w')

step=1
for i in range(0, len(files),step):
    print "week n.", i

    loc_file = files[i:i + step]

    lines = sc.textFile(','.join(loc_file)) \
        .filter(lambda x: validate(x.split(';')[3])) \
        .filter(lambda x: municipio(x.split(';')[9].replace(" ",""))) \
        .map(lambda x: ((x.split(';')[1],cell2municipi[x.split(';')[9].replace(" ","")], x.split(';')[3], x.split(';')[4][:2] ), 1)) \
        .distinct() \
        .reduceByKey(lambda x, y: x + y) \
        .persist(StorageLevel(False, True, False, False))

    chiamate_orarie = lines.map(lambda x: ((x[0][1],x[0][2], x[0][3]), 1)).reduceByKey(lambda x, y: x + y)
    print datetime.datetime.now()
    for l in  chiamate_orarie.collect():
        print >>peaks, "%s,%s,%s,%s"%(l[0][0],l[0][1],l[0][2],l[1])
