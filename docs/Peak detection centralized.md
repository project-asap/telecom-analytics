Peak detection
==============

With this analysis we want to detect relevant peaks representing an event. Comparing the density of population within a region in a given moment against the expected density for that area at that hour of the day can do this. 

The first step of the process consists in defining the geographical area to analyze and to partition it into a set of regions. The same must be done for the time, where a timeframe is chosen (for instance, a month), partitioned into periods (for instance, days) and then into smaller time slots (for instance, hours). Time slots are described by a parameter T, while the regions that cover the area of analysis are described by a parameter S, both parameters being provided by the user. These two parameters, then, allow defining a spatio-temporal grid, and each observation of an input dataset can be assigned to one of its cells. The number of observations that fall in a cell defines its density. The input data is partitioned into two sets: a training dataset and a test dataset. For both datasets the spatiotemporal grid of densities is computed. The first is used to compute the densities of a typical period for each region. The second dataset is then compared against such typical period in order to detect significant deviations.

Based on the densities obtained for each region and each time slot over the training dataset, an expected density value is computed for each region, by averaging the densities measured at the same time slot of all the periods in the time window covered by the dataset. For instance, we might obtain an expected density for each pair (region, hour of the day), i.e., 24 values for each region, assuming 24 one-hour time-slots. 

Then, for each region and each time-slot, the corresponding density is compared against its expected value: if the difference is significant, an event of form (region, weight, time slot) is produced, representing its spatiotemporal slot and a discretized measure (weight) of how strong was the deviation. In particular, events are detected on the base of three parameters:
* a granularity of deviations, expressed as a percentage relative to the expected density; 
* a minimum relative deviation, also expressed as a percentage, used to select significant deviations; 
* an absolute minimum deviation, expressed as an integer number, used to discard extreme cases with very low densities. 

The weights used in defining events will be multiples of the granularity, and an event for a region and a time slot will be built only if the deviation of its density w.r.t. the corresponding expected density is larger than the absolute minimum deviation and in percentage is larger than the minimum relative deviation.

In the following the SQL queries used to implement the analysis are reported. The process assumes to have a “data_raw” table containing the following fields:
* id : an identifier of a tower 
* hour: the hour of the day 
* dow: the day of the week 
* doy: the day of the year
* num: the number of calls started in this tower range at this hour of this specific day.

This information is already aggregated from the CDR which reduce the privacy risks and therefore the data manipulation (in our previous analysis only the 2%of the data was under risk, the remaining 98% was safe to use). Another table used is the “voronoi” table which contains the set of tower ids in analysis.

![Image](/docs/PeakDetectionCentralized.png)

Parameters:
The period to be considered for the training (i.e. doy>=1 and doy<=31)
The period to be considered as test where the peaks will be found (i.e. doy>=32)

The area to be considered: The table voronoi (given by the user) contains the set of towers which are "interesting" for the application and is used only as filter on the data in order to take the CDR in a specific area. To run the experiments on all the data simply remove that "WHERE conditions".

The size of the events ratio bins: In the last qurery the "0.1" in the select is a parameter which determines the binning of the events ratio (i.e. 10%)

Event filtering parameters: The "WHERE conditions" in the last query are parameters to filter events given by the users


DATA FILTER/PREPARATION:

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

TYPICAL DISTRIBUTION COMPUTATION:

    //The table representing the standard behavior, in this case it is computed on the first month of the year. The aggregation reduce everything in a single week (the day of the weeks are not mixed)
    create table cp_base as
    Select rid,hour,dow, avg(num) as num from training_data
    group by rid,hour,dow;


PEAK DETECTION:

    //The peak detection, also called events. Here the variation is computed in percentage of the standard amount of people calling from a tower in a certain hour.
    create table events as
    select  a.rid, a.hour, a.doy, a.dow, 
        ((CAST(a.num AS numeric)/am.num)/
        (CAST(b.num As numeric)/bm.num))
        -1.0 as ratio, a.num as anum, b.num as bnum
    from 
        cp_analyze a, 
        cp_base b, 
        (select doy, dow, sum(num) as num from test_data group by doy, dow) am,
        (select dow, sum(num) as num from cp_base group by dow) bm
    where 
    a.rid=b.rid and 
    a.hour=b.hour and 
    a.dow = b.dow and
    b.dow = bm.dow and
    a.doy = am.doy;

**//After the detection of the peaks they are categorized into bins (in this example 10%) and they are filtered by two constrains: the first one in percentage (minimum 20%) and the second one using an absolute value (minimum 50 calls). An additional constraint can be used to select only the positive variations (ratio>0). The size of bins and the constraints are parameters which may vary the kind of peaks we are interested on.**

    CREATE TABLE events_filter AS
    SELECT rid, hour, doy, dow, floor(abs(ratio/0.1))*0.1*sign(ratio) as ratio 
    FROM events
    WHERE abs(ratio) >= 0.2 AND
    abs(anum-bnum) >= 50 and 
    ratio>0
