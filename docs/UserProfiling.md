User Profiling
==============

The user is profiled starting from the CDR building a matrix representing the its call behaviour. 

![Image](/docs/UserProfiling1.png)

Each user is profiled in each area (i.e. covered by several towers). For example in different area of the city.

![Image](/docs/UserProfiling2.png)

The following query implement the process in SQL.

![Image](/docs/UserProfiling3.png)

Parameters: 
Antenna_areas table (tower_id, area_id)
The time partitioning (i.e 0-8, 8-19, 19-24)
The period (i.e. week>=8 and week<=12)

User Profiling

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
