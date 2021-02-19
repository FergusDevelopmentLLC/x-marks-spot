SELECT longitude, latitude, name, longitude, latitude, geom
FROM public.csv_import_1613448864204
limit 20;

ALTER TABLE csv_import_1613448864204 DROP COLUMN geom;

ALTER TABLE csv_import_1613448864204 ADD COLUMN geom point;

https://postgis.net/install/
CREATE EXTENSION postgis;

SELECT ST_SetSRID(ST_MAKEPOINT(longitude::double precision, latitude::double precision),4326) as geom longitude, latitude, name
FROM public.csv_import_1613448864204
limit 20;


select 
  county.geom,
  county.name, 
  max(pop.pop_2019) as pop_2019,
  count(hospitals.geom) as animal_hospital_count,
  CASE 
    WHEN count(hospitals.geom) = 0 
    THEN max(pop.pop_2019) 
    ELSE max(pop.pop_2019) / count(hospitals.geom) 
  END
  AS persons_per_hospital
from geo_county_raw county
left join population_county pop on pop.name = CONCAT(county.name, ' County')
left join geo_animal_hospitals_usa as hospitals 
  on ST_WITHIN(ST_Transform(hospitals.geom, 4326), ST_Transform(county.geom, 4326))
where pop.state_name = 'Indiana'
and county.statefp = '18'
group by county.geom, county.name
order by persons_per_hospital desc;


SELECT ST_MakePoint(-71.1043443253471, 42.3150676015829);

SELECT ST_MakePoint No function matches the given name and argument types. You might need to add explicit type casts

SELECT ST_SetSRID(ST_MakePoint(-71.1043443253471, 42.3150676015829),4326);


SELECT longitude, latitude, name, geom
  FROM public.csv_import_1613448864204;


SELECT ST_SetSRID(ST_MAKEPOINT(longitude::double precision, latitude::double precision),4326) as geom, longitude, latitude, name
FROM public.csv_import_1613448864204;

select 
  county.geom,
  county.name
  from county county
  where county.statefp = '06';

select * from state where name = 'California';

select 
  county.geom,
  county.name, 
  count(hospitals.geom) as animal_hospital_count
from county county
left join 
	(
	  SELECT ST_SetSRID(ST_MAKEPOINT(longitude::double precision, latitude::double precision),4326) as geom, longitude, latitude, name
	  FROM public.csv_import_1613448864204
	)
	as hospitals on ST_WITHIN(hospitals.geom, county.geom)
where county.statefp = '06'
group by county.geom, county.name
order by animal_hospital_count desc;

CREATE EXTENSION postgis;

CREATE TABLE population_county (
  id SERIAL,
  statefp VARCHAR(2),
  state_name VARCHAR(255),
  name VARCHAR(255),
  type VARCHAR(100),
  pop_2019 integer,
  PRIMARY KEY (id)
);

COPY population_county(id, statefp, state_name, name, type, pop_2019)
FROM '/tmp/county_pop_2019.csv'
DELIMITER ','
CSV HEADER;

select * from 
population_county;





