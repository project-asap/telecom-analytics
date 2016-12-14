DATASET=$1
SPARK_MASTER=$2
export PYTHONPATH=.:$PYTHONPATH
$SPARK_HOME/bin/spark-submit --py-files cdr.py --master $SPARK_MASTER peak_detection/data_filter.py $DATASET spatial_regions/aree_roma.csv roma 2016-01-01 2016-01-31
$SPARK_HOME/bin/spark-submit --master $SPARK_MASTER peak_detection/typical_distribution_computation.py /peaks/hourly_presence roma 2016-01-01 2016-01-31
$SPARK_HOME/bin/spark-submit --master $SPARK_MASTER peak_detection/peak_detection.py /peaks/hourly_presence /peaks/weekly_presence roma 2016-01-01 2016-01-31
