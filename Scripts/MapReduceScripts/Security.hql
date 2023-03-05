show databases;

use default;

CREATE EXTERNAL TABLE IF NOT EXISTS flight_data(
  Year int,
  Month int,
  DayofMonth int,
  DayOfWeek int,
  DepTime int,
  CRSDepTime int,
  ArrTime int,
  CRSArrTime int,
  UniqueCarrier string,
  FlightNum int,
  TailNum string,
  ActualElapsedTime int,
  CRSElapsedTime int,
  AirTime int,
  ArrDelay int,
  DepDelay int,
  Origin string,
  Dest string,
  Distance int,
  TaxiIn int,
  TaxiOut int,
  Cancelled string,
  CancellationCode string,
  Diverted string,
  CarrierDelay int,
  WeatherDelay int,
  NASDelay int,
  SecurityDelay int,
  LateAircraftDelay int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/tables/flight_data';

show tables;

LOAD DATA INPATH 's3://flightassignmentjan/dataset/DelayedFlights-updated.csv' OVERWRITE INTO TABLE flight_data;


SET hive.cli.print.header=true;

SELECT Year As Year, SUM(SecurityDelay) AS YearWiseSecurityDelay
FROM flight_data
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year
ORDER BY Year;

SELECT Year As Year, SUM(SecurityDelay) AS YearWiseSecurityDelay
FROM flight_data
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year
ORDER BY Year;

SELECT Year As Year, SUM(SecurityDelay) AS YearWiseSecurityDelay
FROM flight_data
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year
ORDER BY Year;

SELECT Year As Year, SUM(SecurityDelay) AS YearWiseSecurityDelay
FROM flight_data
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year
ORDER BY Year;

SELECT Year As Year, SUM(SecurityDelay) AS YearWiseSecurityDelay
FROM flight_data
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year
ORDER BY Year;