# Peak detection

This analysis detects relevant peaks representing an event.
This is achieved by comparing the density of population (measured in calls) within a region in a given moment against the expected density for that area at that hour of the day.

The complete workflow involves the usage of the following modules (in the same order):

- [Data Filter] (/src/main/scala/dataFilter/README.md)
- [Distribution Computation] (/src/main/scala/distributionComputation/README.md)
- [Peak Detection] (/src/main/scala/peakDetection/README.md)

The overall calculation is illustrated here:

![Image](/docs/PeakDetection.png)
