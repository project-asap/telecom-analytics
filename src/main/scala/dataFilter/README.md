# Data Filter

This is the first step of the [Peak Detection](../../../../docs/PeakDetection.md) calculation.

The process expects a CDR dataset containing the following fields:
* caller id (masked)
* call date (format: yyyy-MM-dd)
* chargable duration
* tower identifier from which the call has initiated
* tower identifier where the call has ended

Then, the process assumes a dataRaw dataset derived from the above and containing the following fields:
* id : tower identifier from which the call has initiated
* hour: the hour of the day (derived by the call date)
* dow: the day of the week (derived by the call date)
* doy: the day of the year (derived by the call date)
* num: the number of calls started in this tower range at this hour of this specific day.

The next step of the process consists in defining the geographical area to analyze and to partition it into a set of regions.
The same must be done for the time, where a timeframe is chosen (for instance, a month), partitioned into periods (for instance, days) and then into smaller time slots (for instance, hours).
Time slots are described by a parameter T, while the regions that cover the area of analysis are described by a parameter S, both parameters being provided by the user.
These two parameters, then, allow defining a spatio-temporal grid, and each observation of an input dataset can be assigned to one of its cells.
The number of observations that fall in a cell defines its density.
The input data is partitioned into two sets: a training dataset and a test dataset.
For both datasets the spatiotemporal grid of densities is computed.
The first will be used to compute the densities of a typical period for each region.
The second dataset will be then compared against such typical period in order to detect significant deviations.

## Implementation Details

As an spark application, this operator can be executed by being submitted in an running spark installation.
For simplifying the execution [submit.sh] (../../../../submit.sh) can be used.

**Usage**: ./submit.sh ta.DataFilter \<master\> \<cdrPath\> \<output\> \<trainingSince (yyyy-MM-dd)\> \<trainingUntil (yyyy-MM-dd)\> \<testSince (yyyy-MM-dd)\> \<testUntil (yyyy-MM-dd or None)\> \<voronoiPath\>

**Input parameters**:
- the spark master URI
- the input CDR dataset (HDFS or local)
- the ouput path (an non existing HDFS or local directory)
- the start date for the training period (format yyyy-MM-dd)
- the end date for the training period (format yyyy-MM-dd)
- the start date for the test period (format yyyy-MM-dd)
- the end date for the training period (format yyyy-MM-dd) or None
- the path to the voronoi table: the set of towers ids in analysis.

**Output**:
Upon successful execution the \<output\>/trainingData & \<output\>/testData datasets will be created.

e.g.: ./submit.sh ta.DataFilter spark://localhost:7077 /dataset_simulated /output 2015-06-01 2015-06-02 2015-06-03 None /voronoi

## SQL formalization:

    //Converting the CDR (user_id, cell_id, ts) to an aggregated version useful for the analysis
    Create table public.data_raw as
    Select substr(s.cell_id,1,5) as id, extract(hour from ts) as hour, extract(dow from ts) as dow, extract(doy from ts) as doy, count(distinct(user_id)) as num
    From public.cdr
    Group by substr(s.antenna_id,1,5), ts

    Create table training_data as 
    (select id as rid, hour, dow, doy, num from public.data_raw 
    where doy>=1 and doy<=31 and id in (select nidt from voronoi)) t

    Create table test_data as 
    (select id as rid, hour, dow, doy, num from public.data_raw 
    where doy>=32 and id in (select nidt from voronoi)) t
