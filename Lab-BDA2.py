from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 1")
sqlContext = SQLContext(sc)

temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = (year,temperature)
year_temperature = lines.map(lambda x: (x[1][0:4], x[0], float(x[3])))

tempReadingsString = ["year", "station", "temperature"]
# Apply the schema to the RDD.
schema_df = sqlContext.createDataFrame(year_temperature, tempReadingsString)
# Register the DataFrame as a table.
schema_df.registerTempTable("schema_df")

#filter
Schema_df_filtered = schema_df.filter((schema_df["year"]>=1950) & (schema_df["year"]<=2014))

#Get max
max_temperatures = Schema_df_filtered.groupBy("year").agg(F.max("temperature").alias("temperature"))

#Get min
min_temperatures = Schema_df_filtered.groupBy("year").agg(F.min("temperature").alias("temperature"))

#Combine max and min

max_min_temperature = max_temperatures.union(min_temperatures)

#Get stations

final_min_temperatures = max_min_temperature.join(Schema_df_filtered, on=["year", "temperature"], how= "inner").select("year","station","temperature").orderBy("temperature", ascending=[0])

#final_min_temperatures = final_min_temperatures.select("year","station", "minValue").orderBy(["minValue"],descending=[0])
#max_temperatures = Schema_df_filtered.groupBy("year").agg(F.max("temperature").alias("maxValue")).select("year","station","maxValue").orderBy(["maxValue"],descending=[0])


rdd_min = final_min_temperatures.rdd
# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
rdd_min.saveAsTextFile("BDA/output1")

#Output
#Sample of the highest values
"""
Row(year='1975', station='86200', temperature=36.1)
Row(year='1992', station='63600', temperature=35.4)
Row(year='1994', station='117160', temperature=34.7)
Row(year='2010', station='75250', temperature=34.4)
Row(year='2014', station='96560', temperature=34.4)
Row(year='1989', station='63050', temperature=33.9)
"""

#Sample of the lowest values

""" 
Row(year='1980', station='191900', temperature=-45.0)
Row(year='1967', station='166870', temperature=-45.4)
Row(year='1987', station='123480', temperature=-47.3)
Row(year='1978', station='155940', temperature=-47.7)
Row(year='1999', station='192830', temperature=-49.0)
Row(year='1966', station='179950', temperature=-49.4)
"""


#Assignment 2a

sc = SparkContext(appName = "exercise 2")
sqlContext = SQLContext(sc)

# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (year, month, station, temperature))
year_temperature = lines.map(lambda x: (x[1][0:4], x[1][5:7], x[0], (float(x[3]))))

tempReadingsString = ["year","month", "station", "temperature"]
# Apply the schema to the RDD.
schema_df = sqlContext.createDataFrame(year_temperature, tempReadingsString)
# Register the DataFrame as a table.
schema_df.registerTempTable("schema_df")

#filter by year
schema_df_filtered = schema_df.filter((schema_df["year"]>=1960) & (schema_df["year"]<=2014))

#filter by temperature
schema_df_filtered = schema_df_filtered.filter((schema_df_filtered["temperature"] > 10 ))

#Count number of readings for year-month. Thereafter name it value and order it by value.
count_temperatures = schema_df_filtered.groupBy("year","month").agg(F.count("temperature").alias("value")).orderBy("value", ascending=[0])
   
#Convert to rdd to get save as textfile
rdd_year_temperature = count_temperatures.rdd
# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
rdd_year_temperature.saveAsTextFile("BDA/output2a")

#Output
#Sample of output
"""
Row(year='2014', month='07', value=147681)
Row(year='2011', month='07', value=146656)
Row(year='2010', month='07', value=143419)
Row(year='2012', month='07', value=137477)
Row(year='2013', month='07', value=133657)
Row(year='2009', month='07', value=133008)
Row(year='2011', month='08', value=132734)
Row(year='2009', month='08', value=128349)
Row(year='2013', month='08', value=128235) 
"""




#Assignment 2b
sc = SparkContext(appName = "exercise 2")
sqlContext = SQLContext(sc)

# This path is to the file on hdfs
temperature_file = sc.textFile("temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (year, month, station, temperature))
year_temperature = lines.map(lambda x: (x[1][0:4], x[1][5:7], x[0], (float(x[3]))))

tempReadingsString = ["year","month", "station", "temperature"]
# Apply the schema to the RDD.
schema_df = sqlContext.createDataFrame(year_temperature, tempReadingsString)
# Register the DataFrame as a table.
schema_df.registerTempTable("schema_df")

#filter by year
schema_df_filtered = schema_df.filter((schema_df["year"]>=1960) & (schema_df["year"]<=2014))

#filter by temperature
schema_df_filtered = schema_df_filtered.filter((schema_df_filtered["temperature"] > 10 ))
#Count number of readings for year-month. Thereafter name it value and order it by value.
count_temperatures = schema_df_filtered.groupBy("year","month").agg(F.countDistinct("station").alias("value")).orderBy("value", ascending=[0])
   
#Convert to rdd to get save as textfile
rdd_year_temperature = count_temperatures.rdd
# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
rdd_year_temperature.saveAsTextFile("BDA/output2b")

#Output
#Sample of the output

"""
Row(year='1972', month='10', value=378)
Row(year='1973', month='06', value=377)
Row(year='1973', month='05', value=377)
Row(year='1972', month='08', value=376)
Row(year='1973', month='09', value=376)
Row(year='1972', month='09', value=375)
Row(year='1972', month='06', value=375)
Row(year='1972', month='05', value=375)
Row(year='1971', month='08', value=375) 
"""







#Assignment 3

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 3")
sqlContext = SQLContext(sc)

# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = (year, month, day, station), (temperature))
year_temperature = lines.map(lambda x: (x[1][0:4], x[1][5:7], x[1][8:10], x[0], (float(x[3]))))

tempReadingsString = ["year","month", "day", "station", "temperature"]
# Apply the schema to the RDD.
schema_df = sqlContext.createDataFrame(year_temperature, tempReadingsString)
# Register the DataFrame as a table.
schema_df.registerTempTable("schema_df")

#filter by year
schema_df_filtered = schema_df.filter((schema_df["year"]>=1960) & (schema_df["year"]<=2014))


#Get max ordered by temperature
schema_df_filtered_with_max = schema_df_filtered.groupBy("year", "month","day","station").agg(F.max("temperature").alias("max_temperature"))
schema_df_filtered_with_min = schema_df_filtered.groupBy("year", "month","day","station").agg(F.min("temperature").alias("min_temperature"))

schema_df_filtered_with_max_min = schema_df_filtered_with_max.join(schema_df_filtered_with_min, on=["year","month","station"], how="inner")
daily_average_schema_df = schema_df_filtered_with_max_min.withColumn("average_daily_temperature", ((schema_df_filtered_with_max_min["max_temperature"]+schema_df_filtered_with_max_min["min_temperature"])/2))

daily_average_schema_df = daily_average_schema_df.select("year", "month", "station", "average_daily_temperature")

count_temperatures_day = daily_average_schema_df.groupBy("year", "month","station").agg(F.mean("average_daily_temperature").alias("avgMonthlyTemperature"))

final_results = count_temperatures_day.select("year","month","station","avgMonthlyTemperature").orderBy("avgMonthlyTemperature", ascending=[0])


#Convert to rdd to get save as textfile
rdd_year_temperature = final_results.rdd
# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
rdd_year_temperature.saveAsTextFile("BDA/output3")


#Sample of the output
"""
Row(year=u'2014', month=u'07', station=u'96000', avgMonthlyTemperature=26.3)
Row(year=u'1994', month=u'07', station=u'96550', avgMonthlyTemperature=23.071052631578937)
Row(year=u'1983', month=u'08', station=u'54550', avgMonthlyTemperature=23.0)
Row(year=u'1994', month=u'07', station=u'78140', avgMonthlyTemperature=22.9709677419355)
Row(year=u'1994', month=u'07', station=u'85280', avgMonthlyTemperature=22.872580645161207)
Row(year=u'1994', month=u'07', station=u'75120', avgMonthlyTemperature=22.85806451612905)
Row(year=u'1994', month=u'07', station=u'75120', avgMonthlyTemperature=22.85806451612905)
Row(year=u'1994', month=u'07', station=u'65450', avgMonthlyTemperature=22.856451612903285)
Row(year=u'1994', month=u'07', station=u'96000', avgMonthlyTemperature=22.80806451612904)
"""



#Assignment 4
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 4")
sqlContext = SQLContext(sc)

temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines_temperature = temperature_file.map(lambda line: line.split(";"))

precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
lines_precipitation = precipitation_file.map(lambda line: line.split(";"))

# (key, value) = (station, temperature)
station_temperature = lines_temperature.map(lambda x: (x[0], (float(x[3]))))

# (key,value) = (station, precipitation)
station_precipitation = lines_precipitation.map(lambda x: ([1][0:10], x[0], float(x[3])))

tempReadingsString = ["station", "temperature"]

precReadingsString = ["date", "station", "precipitation"]

# Apply the schema to the RDD.
temp_schema_df = sqlContext.createDataFrame(station_temperature, tempReadingsString)
# Register the DataFrame as a table.
temp_schema_df.registerTempTable("temp_schema_df")

# Apply the schema to the RDD.
precipitation_schema_df = sqlContext.createDataFrame(station_precipitation, precReadingsString)
# Register the DataFrame as a table.
precipitation_schema_df.registerTempTable("precipitation_schema_df")

#Get readings
max_temperatures = temp_schema_df.groupBy("station").agg(F.max("temperature").alias("MaxTemperature"))

#precipitation
max_daily_precipitation = precipitation_schema_df.groupBy("station","date").agg(F.sum("precipitation").alias("daily_precipitation"))
max_precipitation = max_daily_precipitation.select("station","daily_precipitation").groupBy("station").agg(F.max("daily_precipitation").alias("MaxPrecipitation"))

#Filter
#by temperature in temperature file
station_temperature = max_temperatures.filter((max_temperatures["MaxTemperature"] => 25) & (max_temperatures["MaxTemperature"] <= 30))

#by precipitation in precipitation file
station_precipitation = max_precipitation.filter((max_precipitation["MaxPrecipitation"] => 100) & (max_precipitation["MaxPrecipitation"] <= 200))


#Join temperature and precipitation

max_temperature_precipitation = station_temperature.join(station_precipitation, on="station", how="inner").orderBy("station", ascending=[0])

#filter by year
#Convert to rdd to get save as textfile
rdd_year_temperature = max_temperature_precipitation.rdd

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
rdd_year_temperature.saveAsTextFile("BDA/output4")

#Output
"""
"""
#Output is empty






#Assignment 5

sc = SparkContext(appName = "exercise 5")
sqlContext = SQLContext(sc)

stations_ostergotland = sc.textFile("BDA/input/stations-Ostergotland.csv")
lines_ostergotland = stations_ostergotland.map(lambda line: line.split(";"))
#Get the station that belongs to ostergotland
ostergotland_stations = lines_ostergotland.map(lambda x: (x[0]))
#Broadcast it according to the description, results in a list with station in ostergotland
ostergotland_broadcasted = sc.broadcast(ostergotland_stations.collect())

precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
lines_precipitation = precipitation_file.map(lambda line: line.split(";"))
station_precipitation = lines_precipitation.map(lambda x: (x[1][0:4], x[1][5:7], x[0], (float(x[3]))))

precipitationReadingsString = ["year", "month", "station", "precipitation"]

# Apply the schema to the RDD.
precipitation_schema_df = sqlContext.createDataFrame(station_precipitation, precipitationReadingsString)
# Register the DataFrame as a table.
precipitation_schema_df.registerTempTable("precipitation_schema_df")

#Filter
#by precipitation in precipitation file
station_precipitation = precipitation_schema_df.filter((1993 <= precipitation_schema_df["year"]) & (precipitation_schema_df["year"] <= 2016))
station_precipitation = station_precipitation.filter((station_precipitation["station"].isin(ostergotland_broadcasted.value)))

#Get readings
#Get total precipitation per station
total_precipitation_ostergotland_stations = station_precipitation.groupBy("year", "month", "station").agg(F.sum("precipitation").alias("Total_precipitation"))

#Get the average of each station
average_precipitation_ostergotland_stations = total_precipitation_ostergotland_stations.groupBy("year", "month").agg(F.mean("Total_precipitation").alias("avgMonthlyPrecipitation"))

results = average_precipitation_ostergotland_stations.select("year", "month", "avgMonthlyPrecipitation").orderBy(["year","month"], descending=[1,1])

rdd_year_temperature = results.rdd

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
rdd_year_temperature.saveAsTextFile("BDA/output5")

#Output
#Sample of the output
"""
Row(year='1993', month='04', avgMonthlyPrecipitation=0.0)
Row(year='1993', month='05', avgMonthlyPrecipitation=21.100000000000005)
Row(year='1993', month='06', avgMonthlyPrecipitation=56.5)
Row(year='1993', month='07', avgMonthlyPrecipitation=95.39999999999999)
Row(year='1993', month='08', avgMonthlyPrecipitation=80.70000000000005)
Row(year='1993', month='09', avgMonthlyPrecipitation=40.6)
Row(year='1993', month='10', avgMonthlyPrecipitation=43.2)
"""
