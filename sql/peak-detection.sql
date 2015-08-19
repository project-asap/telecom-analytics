create table voronoi as select distinct(id) as nidt from cdr limit 10;

create table data_raw as select id, hour, dow, doy, count(*) as num from cdr group by id, hour, dow, doy;

create table cp_base as
Select rid,hour,dow, avg(num) as num from 
--(select id as rid, hour, dow, num from data_raw where doy>=1 and doy<=31) t 
(select id as rid, hour, dow, num from data_raw where doy>=32 and doy<=58) t 
where rid in (select nidt from voronoi)
group by rid, hour, dow;

create table cp_analyze as
select id as rid, hour, doy, dow, num from data_raw
where id in (select nidt from voronoi);

create table events as
select  a.rid, a.hour, a.doy, a.dow, 
--    ((CAST(a.num AS numeric)/am.num)/
--    (CAST(b.num As numeric)/bm.num))
    ((CAST(a.num AS numeric)/(am.num * 1.0))/
    (CAST(b.num As numeric)/(bm.num * 1.0))) -1.0 as ratio,
    a.num as anum,
    b.num as bnum
from 
	cp_analyze a, 
	cp_base b, 
	(select doy, dow, sum(num) as num from cp_analyze group by doy, dow) am,
	(select dow, sum(num) as num from cp_base group by dow) bm
where 
a.rid=b.rid and 
a.hour=b.hour and 
a.dow = b.dow and
b.dow = bm.dow and
a.doy = am.doy;

CREATE TABLE events_filter AS
--SELECT rid, hour, doy, dow, floor(abs(ratio/0.1)) * 0.1 * sign(ratio) as ratio 
SELECT rid, hour, doy, dow, round(abs(ratio/0.1) - 0.5) * 0.1 * (case when ratio < 0 then -1 when ratio = 0 then 0 when ratio > 0 then 1 end) as ratio, abs(ratio), abs(anum-bnum)
FROM events
WHERE abs(ratio) >= 0.2 AND
abs(anum-bnum) >= 50 and 
ratio>0;
