Sociometer
==========

The user is profiled starting from the CDR building a matrix representing the its call behaviour.

![Image](/docs/Sociometer1.png)

Each user is profiled in each area (i.e. covered by several towers). For example in different area of the city.

![Image](/docs/Sociometer2.png)

The complete workflow involves the usage of the following modules (in the same order):

- User Profiling
- Clustering
- User Annotation

The overall calculation is illustrated here:

![Image](/docs/Sociometer3.png)

Pyspark implementation guidelines are available [here] (/src/python/sociometer/README.md).

Scala implementation guidelines are available [here] (/src/main/scala/userProfiling/README.md).
