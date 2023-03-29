use `pollution-db2`;
select year(readings.date_time), stations.location, readings.nox from readings INNER JOIN stations 
ON readings.stations_site_id = stations.site_id AND readings.nox = (SELECT MAX(readings.nox) FROM readings where year(date_time) = '2019');
