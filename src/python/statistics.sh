DATASET=$1
SPARK_MASTER=$2
export PYTHONPATH=.:$PYTHONPATH
$SPARK_HOME/bin/spark-submit --py-files cdr.py --master $SPARK_MASTER statistics/spatio_temporal_aggregation.py $DATASET spatial_regions/aree_roma.csv roma 2016
python visualization/stats_publisher.py timeseries-roma-2016 spatial_regions/roma_gsm.csv roma 2016

