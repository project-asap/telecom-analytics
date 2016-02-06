# Sociometer modules documentation –pyspark

The complete workflow involves the usage of the following modules (in the same order).

## User profiling

**Usage**: pyspark user_profilo.py \<folder\> \<spatial_division\> \<region\> \<timeframe\> \<profiles_out\>

**e.g.**: pyspark user_profilo.py dataset centro_roma.csv roma 06-2015 /profiles

Input parameters:

- **folder**: the hdfs folder where the dataset is located. In order to let the profiles be computed, it needs at least 3 weeks of data. Dataset is assumed to be splitted into days (e.g. one day = one csv file).
- **spatial_division**: A csv file containing the spatial region of each GSM tower. E.g. “***REMOVED***;city_center”.
- **region**: a string containing the name of the region related to the dataset
- **timeframe**: a string containing the period related to the dataset
- **profiles_out**: the hdfs folder **prefix** where the output dataset will be stored.

Output:

It stores the profiles (as Pickle file) into the folder \<profiles_out\>/\<region\>-\<timeframe\>.

Profiles are in the format: user_id->[(region,week n.,workday/weekend, timeframe,number of presence),….]

## Clustering

**Usage**: pyspark clustering.py \<profiles_in\> \<archetipi\> \<k\> \<percentage\> \<centroids_out\>

**e.g.**: pyspark clustering.py /profiles/roma-06-2015 roma 06-2015 archetipi.csv 100 0.4 /centroids

Input parameters:

- **profiles_in**: the hdfs folder **prefix** where the profiles dataset produced by the previous step (User profiling) is located.
    This value should have the following format:

        */<region>-<timeframe>

    where **region** and **timeframe** are the parameters passed to the previous step in order to create the profiles.

- **archetipi**: a csv files containing typical calling profiles for each label. E.g.: Resident->typical resident profiles, etc..
- **k**: the number of centroids to be computed
- **percentage**: the percentage of profiles to use for centroids computation
- **centroids_out**: the hdfs folder **prefix** where the output dataset will be stored.

Output:

It stores a files \<centroids_out\>/\<region\>-\<timeframe\> containing the association between each centroid and the user type. E.g. Centroid1->resident, etc

## User annotation

**Usage**: pyspark user_annotation.py \<profiles_in\> \<centroids_in\> \<sociometer_out\>

**e.g.**: pyspark user_annotation.py /profiles/roma-06-2015 /centroids/roma-06-2015 /sociometer

Input parameters:

- **profiles_in**: the hdfs folder **prefix** where the profiles dataset produced by the first step (User profiling) is located.
- **centroids_in**: the hdfs folder **prefix** where the profiles dataset produced by the previous step (Clustering) is located.
- **sociometer_out**: the hdfs folder **prefix** where the output dataset will be stored.

Output:
It stores a file “\<sociometer_out\>/\<region\>-\<timeframe\>” containing the percentage of user of each profile.
E.g.  roma-center, Resident, 0.34

Note that the **region** and **timeframe** in the above path are the parameters passed to the firest step in order to create the profiles.
