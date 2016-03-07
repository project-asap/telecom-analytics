__author__ = 'paul'
import datetime
from pyspark import SparkContext
import hdfs

"""
Data Filter Module

Given a CDR dataset and a set of geographical regions, it returns the hourly presence for each region.

Usage: data_filter.py <folder> <spatial_division> <region> <timeframe>

--folder: hdfs folder where the CDR dataset is placed
--spatial division: csv file with the format GSM tower id --> spatial region
--region,timeframe: file name desired for the stored results. E.g. Roma 11-2015

example: pyspark data_filter.py /dataset_simulated/06 ../spatial_regions/aree_roma.csv roma 06-215

Results are stored into hdfs: /peaks/hourly_presence-<region>-<timeframe>

"""


import time
import os,sys
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
        datetime.datetime.strptime(date_text, '%Y%m%d')
        return True
    except ValueError:
        return False
def week_month(string):
    #settimana del mese
    d=datetime.datetime.strptime(string, '%Y%m%d')
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

###data loading
#checking file existance
#####
sc=SparkContext()
quiet_logs(sc)
sc._conf.set('spark.executor.memory','24g').set('spark.driver.memory','24g').set('spark.driver.maxResultsSize','0')
file_path='hdfs://hdp1.itc.unipi.it:9000/%s'%folder
print file_path
files=[]
nfile=[]
for x in hdfs.ls("/"+folder)[:]:
	files.append("hdfs://hdp1.itc.unipi.it:9000%s"%(x))

start=time.time()
rddlist=[]
if len(files)%7!=0:
    print "missing complete weeks in dataset"
    #exit()


#plot utenti con chiamate durante partite
for i in range(0, len(files), 3):
    print "week n.", i

    loc_file = files[i:i + 3]

    lines = sc.textFile(','.join(loc_file),10) \
        .filter(lambda x: validate(x.split(';')[3])) \
        .filter(lambda x: municipio(x.split(';')[9].replace(" ",""))) \
        .map(lambda x: ((x.split(';')[1],cell2municipi[x.split(';')[9].replace(" ","")],day_of_week( x.split(';')[3]), week_month(x.split(';')[3]),x.split(';')[4][:2], x.split(';')[3]), 1)) \
        .distinct() \
        .reduceByKey(lambda x, y: x + y)

    rddlist.append(lines)

for r in range(len(rddlist) - 1):
    rddlist[r + 1] = rddlist[r].union(rddlist[r + 1])

###

### risultato dei join dei veri RDD
r = rddlist[-1].reduceByKey(lambda x, y: x + y)
chiamate_orarie = r.map(lambda x: ((x[0][1],x[0][2], x[0][3],x[0][4],x[0][5]), 1.0)).reduceByKey(lambda x, y: (x + y))


os.system("$HADOOP_HOME/bin/hadoop fs -rm -r /peaks/hourly_presence-%s-%s/" %(region,timeframe))
chiamate_orarie.saveAsPickleFile('hdfs://hdp1.itc.unipi.it:9000/peaks/hourly_presence-'+"%s-%s"%(region,timeframe))
