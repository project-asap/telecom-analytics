User Profiling
==============

The user is profiled starting from the CDR building a matrix representing the its call behaviour.

## Implementation Details

Usage:

./submit.sh ta.UserCallProfiling \<master\> \<cdrIn\> \<geoIn\> \<profilingOut\> \<eventsFilterOut\>\<baseSince\> \<baseUntil\>


**Input parameters**:
- **master**: the spark master URI
- **cpBaseIn**: the input cdr dataset URI (HDFS or local)
- **geoIn**: the input geographical dataset URI (HDFS or local)
- **profilingOut**: the output profiles dataset URI (an non existing HDFS or local directory)
- **baseSince**: the start date for the period under analysis (format yyyy-MM-dd)
- **baseUntil**: the end date for the period under analysis (format yyyy-MM-dd) or None


## SQL formalization:

    create table user_profiles as
    select user_id, area_id, week, weekday, time_slot, count(distinct(week,weekday,_day)) as count
        From
            (
            SELECT distinct user_id, area_id, extract (week from ts) as week, 
            case when extract (isodow from ts) <6 then '0'::integer else '1'::integer end as weekday , 
            case when ts::time between timestamp '2012-01-01 00:00:00'::time and  timestamp '2012-01-01 07:59:59'::time then '1'::integer
            when ts::time between timestamp '2012-01-01 08:00:00'::time and  timestamp '2012-01-01 18:59:59'::time then '2'::integer
            when ts::time between timestamp '2012-01-01 19:00:00'::time and  timestamp '2012-01-01 23:59:59'::time then '3'::integer
            end as time_slot,
            date_trunc('day',ts) as _day, ts
            FROM cdr_data s, antenne_areas g
                    where substr(s.antenna_id,1,5)=g.tower_id and week >= 8 and week <= 12
            ) as a
        group by 1,2,3,4,5
        order by user_id, week, weekday, time_slot;
