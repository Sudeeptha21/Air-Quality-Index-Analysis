
------Setup-------
create table daily_aqi_by_county
(
    state_name  VARCHAR(50),
    county_name  VARCHAR(50),
    state_code INT,
    county_code INT,
    date date,
    aqi INT,
    category  VARCHAR(50),
    defining_parameter  VARCHAR(50),
    defining_site  VARCHAR(50),
    number_of_sites_reporting INT
    
)

copy daily_aqi_by_county(
    state_name,
    county_name,
    state_code,
    county_code,
    date,
    aqi,
    category,
    defining_parameter,
    defining_site,
    number_of_sites_reporting
	)
from 'path\to\daily_aqi_by_county_2024\daily_aqi_by_county_2024.csv'
delimiter  ',' CSV header ;

copy daily_aqi_by_county(
    state_name,
    county_name,
    state_code,
    county_code,
    date,
    aqi,
    category,
    defining_parameter,
    defining_site,
    number_of_sites_reporting
	)
from 'path\to\daily_aqi_by_county_2014\daily_aqi_by_county_2014.csv'
delimiter  ',' csv header ;

--2004 csv has non-integer values for state code and county code which need to be converted to integer

create temp table temp_aqi_data (
    state_name VARCHAR(50),
    county_name VARCHAR(50),
    state_code VARCHAR(10),
    county_code VARCHAR(10),
    date DATE,
    aqi INT,
    category VARCHAR(50),
    defining_parameter VARCHAR(50),
    defining_site VARCHAR(50),
    number_of_sites_reporting INT 
);



copy temp_aqi_data(
    state_name,
    county_name,
    state_code,
    county_code,
    date,
    aqi,
    category,
    defining_parameter,
    defining_site,
    number_of_sites_reporting
	)
from 'path\to\daily_aqi_by_county_2004\daily_aqi_by_county_2004.csv'
DELIMITER ',' CSV HEADER;

--forcing conversion to INTvalues
INSERT INTO daily_aqi_by_county(state_name, county_name, state_code, county_code, date, aqi, category, defining_parameter, defining_site, number_of_sites_reporting)
SELECT 
    state_name, county_name, 
    CAST(state_code AS INT), 
     CAST(state_code AS INT), 
    date,aqi, category, defining_parameter, defining_site, number_of_sites_reporting
FROM temp_aqi_data
WHERE state_code ~ '^[0-9]+$';

-----Cleaning the data------
--Duplicating the table
CREATE TABLE air_quality_data_cleaned AS
SELECT DISTINCT *
FROM daily_aqi_by_county;

--  Filtering out AQI values below 0 and above 500
DELETE FROM air_quality_data_cleaned
WHERE aqi <= 0 OR aqi > 500;

-- Renaming the table to the original name
DROP TABLE daily_aqi_by_county;
ALTER TABLE air_quality_data_cleaned RENAME TO daily_aqi_by_county;
	
Select count(*) from daily_aqi_by_county;

------End Setup-----

------Analysis-------

-- Average AQI (air quality index) by year by season (winter, spring, summer, fall)
select year, season, ROUND(AVG(aqi),2) as average_aqi
from (select aqi, extract (year from DATE) as year, 
    case  
        WHEN EXTRACT(MONTH FROM date) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM date) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM date) IN (6, 7, 8) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM date) IN (9, 10, 11) THEN 'Fall'
    END AS season
    from daily_aqi_by_county 
    )
GROUP BY year, season
ORDER BY year,
	CASE season 
        WHEN 'Summer' THEN 1 
        WHEN 'Spring' THEN 2 
        WHEN 'Fall' THEN 3 
        WHEN 'Winter' THEN 4 
END;


--Top 10 locations with worst AQI in each year
WITH ranked_county_aqi AS (
    SELECT 
       county_name, state_name, aqi,
        extract (year FROM DATE) AS YEAR,
        ROW_NUMBER() OVER (PARTITION BY EXTRACT(YEAR FROM DATE), state_name, county_name ORDER BY AQI DESC) AS rn
    FROM daily_aqi_by_county
    WHERE EXTRACT(YEAR FROM DATE) IN (2004, 2014, 2024)
),
top_aqi AS (
    SELECT state_name, county_name, AQI, YEAR,
        ROW_NUMBER() OVER (PARTITION BY YEAR ORDER BY AQI DESC) AS rank
    FROM ranked_county_aqi
    WHERE rn = 1 
)
SELECT county_name,state_name, AQI, YEAR
FROM top_aqi
WHERE rank <= 10  
ORDER BY YEAR, AQI DESC;

-- Top 10 locations that had the best improvement over 20 years
with AQI_CHANGE as (
	select county_name, state_name,  extract (year from  DATE) as year , AVG(AQI) AS avg_aqi
	FROM DAILY_AQI_BY_COUNTY
	WHERE EXTRACT(YEAR FROM DATE) IN (2004, 2024)
	GROUP BY YEAR, state_name, county_name
)
SELECT FY.county_name, FY.state_name,  ROUND(FY.avg_aqi, 2) AS aqi_2004, ROUND(CY.avg_aqi, 2) AS aqi_2024,
ROUND(FY.avg_aqi - CY.avg_aqi, 2) AS improvement
from  AQI_CHANGE FY
join AQI_CHANGE CY
on 
FY.state_name = CY.state_name and 
FY.county_name = CY.county_name and  
FY.year  = 2004 and CY.year  = 2024
where FY.avg_aqi > CY.avg_aqi
order by improvement desc 
limit 10;

--Top 10 locations with the worst decline over 20 years?
WITH AQI_CHANGE AS(
	select state_name, county_name, extract (year from DATE) AS year, AVG(aqi) AS avg_aqi
	from DAILY_AQI_BY_COUNTY
	where extract (year from DATE) in (2004, 2024)
	group by year, state_name, county_name
)
select FY.county_name,  FY.state_name, ROUND(FY.avg_aqi, 2) AS aqi_2004, ROUND(CY.avg_aqi, 2) AS aqi_2024,
ROUND (CY.avg_aqi - FY.avg_aqi , 2) AS decline
from AQI_CHANGE FY
join AQI_CHANGE CY
on 
FY.state_name = CY.state_name and 
FY.county_name = CY.county_name and  
FY.year  = 2004 and  CY.year  = 2024
where FY.avg_aqi < CY.avg_aqi
order by decline desc 
limit 10;

--Number of  days with "Unhealthy" air quality in Utah counties  
	select extract (year from DATE) AS year,
	    county_name, COUNT(*) AS Unhealthy_Days
	from  daily_aqi_by_county
	where AQI > 150 and AQI <= 200
	    and  state_name = 'Utah'
	group  by county_name, year 
	order by year, county_name ;

-- Months with the most number of "Unhealthy" days in Salt Lake County 
select  
    extract (year from DATE) as year,
    extract (month from  DATE) as  month,
    COUNT (*) as  Unhealthy_Days
from daily_aqi_by_county
where AQI > 150 and AQI <= 200
    and county_name = 'Salt Lake'
group by year , month 
order by year , month ;





