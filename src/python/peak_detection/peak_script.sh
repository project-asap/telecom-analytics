$SPARK_HOME/bin/pyspark data_filter.py dataset_simulated/06 ../spatial_regions/aree_roma.csv roma 06-2015
$SPARK_HOME/bin/pyspark typical_distribution_computation.py roma 06-2015
$SPARK_HOME/bin/pyspark peak_detection.py ../spatial_regions/aree_roma.csv roma 06-2015
