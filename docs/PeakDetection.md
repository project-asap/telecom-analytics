# Peak detection

This analysis detects relevant peaks representing an event.
This is achieved by comparing the density of population (measured in calls) within a region in a given moment against the expected density for that area at that hour of the day.

The overall analysis is decomposed in the following operators:

- [Data Filter] (/src/main/scala/dataFilter/README.md)
- [Distribution Computation] (/src/main/scala/distributionComputation/README.md)
- [Peak Detection] (/src/main/scala/peakDetection/README.md)

The above operators are implemented as Spark applications (using the scala API) and they can be executed by submitting them to a running Spark installation.

The overall calculation is illustrated here:

![Image](/docs/PeakDetection.png)
