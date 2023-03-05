import time
from pyspark.sql import SparkSession

S3_DATA_SOURCE_PATH = "s3://flightassignmentjan/dataset/DelayedFlights-updated.csv"
S3_OUTPUT_PATH = "s3://flightassignmentjan/output"

spark = SparkSession.builder.appName('assignmentApp').getOrCreate()
df = spark.read.format("csv").option("header", "true").load(S3_DATA_SOURCE_PATH)
df.createOrReplaceTempView("df")


def main():
    carrier_delay_query().show()
    nas_delay_query().show()
    weather_delay_query().show()
    late_delay_query().show()
    security_delay_query().show()


def carrier_delay_query():
    carrier_delay = None
    for i in range(0, 5):
        start = time.time()
        carrier_delay = spark.sql("""
        SELECT Year As Year, SUM(CarrierDelay) As YearWiseCarrierDelay
        FROM df
        WHERE Year >= 2003 AND Year <= 2010
        GROUP BY Year
        ORDER BY Year""")
        end = time.time()
        duration = end - start
        print(i+1, " iteration duration for carrier delay ", duration)
    return carrier_delay


def nas_delay_query():
    nas_delay = None

    for i in range(0, 5):
        start = time.time()
        nas_delay = spark.sql("""
        SELECT Year As Year, SUM(NASDelay) As YearWiseNASDelay
        FROM df
        WHERE Year >= 2003 AND Year <= 2010
        GROUP BY Year
        ORDER BY Year""")
        end = time.time()
        duration = end - start
        print(i+1, " iteration duration for nas delay ", duration)
    return nas_delay


def weather_delay_query():
    weather_delay = None
    for i in range(0, 5):
        start = time.time()
        weather_delay = spark.sql("""
        SELECT Year As Year, SUM(WeatherDelay) As YearWiseWeatherDelay
        FROM df
        WHERE Year >= 2003 AND Year <= 2010
        GROUP BY Year
        ORDER BY Year""")
        end = time.time()
        duration = end - start
        print(i+1, " iteration duration for weather delay ", duration)
    return weather_delay


def late_delay_query():
    late_delay = None
    for i in range(0, 5):
        start = time.time()
        late_delay = spark.sql("""
        SELECT Year As Year, SUM(LateAircraftDelay) As YearWiseLateAircraftDelay
        FROM df
        WHERE Year >= 2003 AND Year <= 2010
        GROUP BY Year
        ORDER BY Year""")
        end = time.time()
        duration = end - start
        print(i+1, " iteration duration for late delay ", duration)
    return late_delay


def security_delay_query():
    security_delay = None
    for i in range(0, 5):
        start = time.time()
        security_delay = spark.sql("""
        SELECT Year As Year, SUM(SecurityDelay) As YearWiseSecurityDelay
        FROM df
        WHERE Year >= 2003 AND Year <= 2010
        GROUP BY Year
        ORDER BY Year""")
        end = time.time()
        duration = end - start
        print(i+1, " iteration duration for security delay ", duration)
    return security_delay


if __name__ == '__main__':
    main()
