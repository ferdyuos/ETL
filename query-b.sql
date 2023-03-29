use `pollution-db2`;
select year(date_time) as year, avg(r.`pm2.5`) as pm25_average ,avg(r.`vpm2.5`) as vpm25_average, s.location as location from readings r INNER JOIN stations s ON r.stations_site_id=s.site_id where year(r.date_time)='2019' and  time(r.date_time) BETWEEN '07:50:00' AND '08:10:00' group by s.location; 
