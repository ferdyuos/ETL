use `pollution-db2`;

-- without inner join
-- select year(readings.date_time) as year, avg(readings.`pm2.5`) as pm25_average,avg(readings.`vpm2.5`) as vpm25_average, stations.location as location  from readings, stations where year(readings.date_time)='2019' and  and time(readings.date_time) BETWEEN '07:50:00' AND '08:10:00' and readings.stations_site_id=stations.site_id group by stations.location;

-- using inner join
select year(r.date_time) as year, avg(r.`pm2.5`) as pm25_average, avg(r.`vpm2.5`) as vpm25_average, s.location  as location from readings  r INNER JOIN stations s ON r.stations_site_id=s.site_id where year(r.date_time) BETWEEN '2010' AND '2019' and time(r.date_time) BETWEEN '07:50:00' AND '08:10:00' group by s.location; 
