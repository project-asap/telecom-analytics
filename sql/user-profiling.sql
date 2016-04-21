--
--Copyright 2015-2016 WIND,FORTH
--
--Licensed to the Apache Software Foundation (ASF) under one
--or more contributor license agreements.  See the NOTICE file
--distributed with this work for additional information
--regarding copyright ownership.  The ASF licenses this file
--to you under the Apache License, Version 2.0 (the
--"License"); you may not use this file except in compliance
--with the License.  You may obtain a copy of the License at
--
--  http://www.apache.org/licenses/LICENSE-2.0
--
--Unless required by applicable law or agreed to in writing,
--software distributed under the License is distributed on an
--"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
--KIND, either express or implied.  See the License for the
--specific language governing permissions and limitations
--under the License.
--
create table wind201403.user_profile_by_lg_lucca as
select id_chiamante, comune, week, weekday, time_slot, count(distinct(week,weekday,_day)) as count
From
    (
    SELECT distinct id_chiamante, comune, extract (week from ts_start) as week, 
    case when extract (isodow from ts_start) <6 then '0'::integer else '1'::integer end as weekday , 
    case when ts_start::time between timestamp '2012-01-01 00:00:00'::time and  timestamp '2012-01-01 07:59:59'::time then '1'::integer
    when ts_start::time between timestamp '2012-01-01 08:00:00'::time and  timestamp '2012-01-01 18:59:59'::time then '2'::integer
    when ts_start::time between timestamp '2012-01-01 19:00:00'::time and  timestamp '2012-01-01 23:59:59'::time then '3'::integer
    end as time_slot,
    date_trunc('day',ts_start) as _day, ts_start
    FROM wind201403.cdr_lucca s, geom.antenne_201310_comuni_toscani g
            where substr(s.id_cella_start,1,5)=g.cell_id and week >= 8 and week <= 12
    ) as a
group by 1,2,3,4,5
order by id_chiamante, week, weekday, time_slot;
