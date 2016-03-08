from pyspark import SparkContext

"""
Stereo Type Classification  Module

Given a set of users' profiles and a set of labeled calling behaviors, it returns the percentage of each label
on each spatial region.
E.g.:
Region 1, resident, 75%
Region 1, commuter, 20%
...


Usage: stereo_type_classification.py  <region> <timeframe>

--region,timeframe: file name desired for the stored results. E.g. Roma 11-2015

example: pyspark stereo_type_classification.py roma 06-2015

Results are stored into file: sociometer-<region>-<timeframe>.csv

"""


import sys
region = sys.argv[1]
timeframe = sys.argv[2]


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def euclidean(v1, v2):
    return sum([(v1[i] - v2[i])**2 for i in range(len(v1))])**0.5


def annota_utente(profilo, profiles, id):
    def f7(seq):
        seen = set()
        seen_add = seen.add
        return [x for x in seq if not (x in seen or seen_add(x))]
    for munic in set([x[0] for x in profilo]):
        # settimana,work/we,timeslice, count normalizzato
        week_ordering = f7([x[1] for x in profilo if x[0] == munic])

        obs = [x[1:] for x in profilo if x[0] == munic]
        # carr=np.zeros(24)
        carr = [0 for x in range(18)]
        for o in obs:
            week_idx = week_ordering.index(o[0])
            idx = (week_idx - 1) * 6 + o[1] * 3 + o[2]
            carr[idx] = o[3]
        tipo_utente = sorted([(c[0], euclidean(carr, list(c[1])))
                              for c in profiles], key=lambda x: x[1])[0][0]
        if len([x for x in carr if x != 0]) == 1:
            tipo_utente = 'passing by'
        yield (id, munic, tipo_utente)


sc = SparkContext()
quiet_logs(sc)
# annotazione utenti

# open
r = sc.pickleFile('/centroids-%s-%s' % (region, timeframe))
cntr = r.collect()

profiles = [(x[0], x[1]) for x in cntr]

r = sc.pickleFile('/profiles-%s-%s' % (region, timeframe))
r_id = r.flatMap(lambda x:  annota_utente(x[1], profiles, x[0])).collect()

userid = open('user_id-%s-%s.csv' % (region, timeframe), 'w')
for x in r_id:
    print >> userid, "%s;%s;%s" % x
exit()
r_auto = r.flatMap(lambda x:  annota_utente(x[1], profiles)) \
    .map(lambda x: ((x[0], x[1]), 1)) \
    .reduceByKey(lambda x, y: x + y)
#
# ottengo coppie municipio,id_cluster
# risultato finale
#
lst = r_auto.collect()
sociometer = [(x[0], x[1] * 1.0 / sum([y[1]
                                       for y in lst if y[0][0] == x[0][0]])) for x in lst]
outfile = open('sociometer-%s-%s' % (region, timeframe), 'w')
print >>outfile, "municipio, profilo, percentage"
for s in sorted(sociometer, key=lambda x: x[0][0]):
    print>>outfile, s[0][0], s[0][1].replace("\n", ""), s[1]
