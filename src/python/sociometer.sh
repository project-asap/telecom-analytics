DATASET=$1
SPARK_MASTER=$2
export PYTHONPATH=.:$PYTHONPATH
$SPARK_HOME/bin/spark-submit --py-files cdr.py --master $SPARK_MASTER sociometer/user_profiling.py $DATASET spatial_regions/aree_roma.csv roma 2016-01-01 2016-01-31
$SPARK_HOME/bin/spark-submit --master $SPARK_MASTER sociometer/clustering.py /profiles roma 2016-01-01 2016-01-31
$SPARK_HOME/bin/spark-submit --master $SPARK_MASTER sociometer/stereo_type_classification.py /profiles /centroids roma 2016-01-01 2016-01-31
