#Assignment 1
from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = (year,temperature)
year_temperature = lines.map(lambda x: (x[1][0:4], (float(x[3]))))

#filter by year
year_temperature = year_temperature.filter(lambda x: int(x[0])>=1950 and int(x[0])<=2014)

#Get max
max_temperatures = year_temperature.reduceByKey(lambda a,b: a if a>=b else b)
#Get min
min_temperatures = year_temperature.reduceByKey(lambda a,b: a if a<=b else b)
#Combine them together to one RDD
max_min_temperatures = max_temperatures.union(min_temperatures)
#Sort them by temperature
max_min_temperatures = max_min_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1])


# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
max_min_temperatures.saveAsTextFile("BDA/output")

#Output
#Lowest values
""" (u'1986', -44.2)
(u'1971', -44.3)
(u'1956', -45.0)
(u'1980', -45.0)
(u'1967', -45.4)
(u'1987', -47.3)
(u'1978', -47.7)
(u'1999', -49.0)
(u'1966', -49.4) """

#Highest values
""" (u'1975', 36.1)
(u'1992', 35.4)
(u'1994', 34.7)
(u'2014', 34.4)
(u'2010', 34.4)
(u'1989', 33.9)
(u'1982', 33.8)
(u'1968', 33.7) """


#Assignment 2A

from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = (year, month), (station, temperature))
year_temperature = lines.map(lambda x: ((x[1][0:4],x[1][5:7]), (x[0], float(x[3]))))

#Filter
#by year
year_temperature = year_temperature.filter(lambda x: int(x[0][0])>=1950 and int(x[0][0])<=2014)
#by temperature
year_temperature = year_temperature.filter(lambda x: int(x[1][1])>10)
#Delete temperature (as we don't need it anymore)

#Get readings
# We can set value equal to 1. And for each new kew we sum up the values.
count_temperatures = year_temperature.map(lambda x: ((x[0][0],x[0][1]), 1)).reduceByKey(lambda a,b: a+b)


# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
count_temperatures.saveAsTextFile("BDA/output2a")

#Output
#Sample of output0
""" ((u'2008', u'11'), 446)
((u'1971', u'06'), 42375)
((u'1966', u'11'), 105)
((u'1956', u'05'), 10533)
((u'1998', u'07'), 113635)
((u'1975', u'02'), 10)
((u'1983', u'10'), 7172)
((u'1992', u'05'), 30436)
((u'1969', u'10'), 9841)
((u'1994', u'10'), 3457)
((u'2008', u'02'), 20)
((u'1976', u'07'), 62025)
((u'1989', u'01'), 16)
((u'2007', u'10'), 11112)
((u'1959', u'08'), 23084)
((u'1996', u'09'), 34469)
((u'1973', u'08'), 56521)
((u'1952', u'09'), 4267) """



#Assignment 2B

from pyspark import SparkContext

sc = SparkContext(appName = "exercise 2")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = (year, month), (station, temperature))
year_temperature = lines.map(lambda x: ((x[1][0:4],x[1][5:7]), (x[0], float(x[3]))))

#Filter
#by year
year_temperature = year_temperature.filter(lambda x: int(x[0][0])>=1950 and int(x[0][0])<=2014)
#by temperature
year_temperature = year_temperature.filter(lambda x: int(x[1][1])>10)
#Delete temperature (as we don't need it anymore)
#Thereafter we can get unique elements by using distinct() function.

year_temperature = year_temperature.map(lambda x: ((x[0][0],x[0][1]), x[1][0])).distinct()
    
#Get readings
#As we dont have any duplicates, we can therefore set value equal to 1. And for each new kew we sum up the values.
count_temperatures = year_temperature.map(lambda x: ((x[0][0],x[0][1]), 1)).reduceByKey(lambda a,b: a+b)

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
count_temperatures.saveAsTextFile("BDA/output2B")

#Output
#Sample of the output
""" ((u'2008', u'11'), 52)
((u'1971', u'06'), 374)
((u'1966', u'11'), 50)
((u'1956', u'05'), 124)
((u'1998', u'07'), 326)
((u'1975', u'02'), 7)
((u'1983', u'10'), 233)
((u'1992', u'05'), 311)
((u'1969', u'10'), 336)
((u'1994', u'10'), 240)
((u'1988', u'06'), 322)
((u'1976', u'07'), 356)
((u'1965', u'07'), 349)
((u'1989', u'01'), 6)
((u'2007', u'10'), 242)
((u'1959', u'08'), 125)
((u'1996', u'09'), 337)
((u'2008', u'02'), 7)
((u'1952', u'09'), 114)
((u'2011', u'08'), 316)
((u'1975', u'11'), 79)
((u'1950', u'10'), 44) """


#Assignment 3
sc = SparkContext(appName = "exercise 3")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = (year, month, day, station), (temperature))
year_temperature = lines.map(lambda x: ((x[1][0:4], x[1][5:7], x[1][8:10], x[0]), (float(x[3]))))

#Filter
#by year
year_temperature = year_temperature.filter(lambda x: int(x[0][0])>=1960 and int(x[0][0])<=2014)
        
#Get readings
#First through mapvalues additional value equal to 1 is created, which we will call counter.
#Thereafter, all the daily temperatures are summed up and all counters are summed up (to get a total count).
#Lastly, sum of temperature is divided by the counter to get the average daily temperature.
    
count_temperatures_day = year_temperature.mapValues(lambda value: (value, 1)).reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])).mapValues(lambda value: value[0]/value[1])

#To get it into monthly, we delete the day from the key.
#Thereafter, the same procedure is used to get the monthly average per station.

count_temperatures_monthly = count_temperatures_day.map(lambda x: ((x[0][0],x[0][1],x[0][3]), x[1])).mapValues(lambda value: (value, 1)).reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])).mapValues(lambda value: value[0]/value[1])
    
# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder    
count_temperatures_monthly.saveAsTextFile("BDA/output3")

#Output
#Sample of the output

""" ((u'1989', u'06', u'92400'), 14.93611111111111)
((u'1982', u'09', u'107530'), 11.119166666666665)
((u'2002', u'11', u'136360'), -5.832222222222223)
((u'1967', u'08', u'98170'), 15.38494623655914)
((u'2002', u'08', u'181900'), 15.644086021505379)
((u'1981', u'11', u'63440'), 3.154444444444445)
((u'1996', u'08', u'96190'), 17.291129032258066)
((u'1994', u'06', u'71180'), 13.017916666666668)
((u'2010', u'10', u'64130'), 5.974193548387097)
((u'1995', u'06', u'62400'), 16.13)
((u'1972', u'10', u'64130'), 7.626881720430107)
((u'1985', u'02', u'81130'), -7.489047619047619)
((u'1977', u'10', u'191900'), -2.665322580645162)
((u'1988', u'04', u'86330'), 4.582222222222223)
((u'1989', u'04', u'180940'), -0.20188438487523142)
((u'1992', u'09', u'92100'), 11.592222222222222)
((u'1964', u'04', u'53640'), 7.714444444444444) """

#Assignment 4
sc = SparkContext(appName = "exercise 4")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines_temperature = temperature_file.map(lambda line: line.split(";"))

precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
lines_precipitation = precipitation_file.map(lambda line: line.split(";"))

# (key, value) = (station, temperature)
station_temperature = lines_temperature.map(lambda x: (x[0], (float(x[3]))))

# (key,value) = (station, precipitation)
station_precipitation = lines_precipitation.map(lambda x: (x[0], (float(x[3]))))

#Filter
#by temperature in temperature file
station_temperature = station_temperature.filter(lambda x: float(x[1]) >= 25 and float(x[1]) <= 30)
    
#by precipitation in precipitation file
station_precipitation = station_precipitation.filter(lambda x: float(x[1]) >= 100 and float(x[1]) <= 200)

#Get readings
max_temperatures = station_temperature.reduceByKey(lambda a,b: a if a>=b else b)
max_precipitation = station_precipitation.reduceByKey(lambda a,b: a if a>=b else b)

#Join temperature and precipitation
max_temperature_precipitation = max_temperatures.join(max_precipitation)

#The join returns empty as there are no values between 100-200. It can be seen after the filtering.

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
max_temperature_precipitation.saveAsTextFile("BDA/output4")

#Output
#Comment: Output is empty due to the precipitation filtering, as it results in empty rdd and when joined the results are empty.


#Assignment 5
sc = SparkContext(appName = "exercise 1")
    
# This path is to the file on hdfs
stations_ostergotland = sc.textFile("BDA/input/stations-Ostergotland.csv")
lines_ostergotland = stations_ostergotland.map(lambda line: line.split(";"))
#Get the station that belongs to ostergotland
ostergotland_stations = lines_ostergotland.map(lambda x: (x[0]))
#Broadcast it according to the description, results in a list with station in ostergotland
ostergotland_broadcasted = sc.broadcast(ostergotland_stations.collect())

precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
lines_precipitation = precipitation_file.map(lambda line: line.split(";"))

# (key,value) = ((year, month, station), precipitation)
station_precipitation = lines_precipitation.map(lambda x: ((x[1][0:4], x[1][5:7], x[0]), (float(x[3]))))

#Filter  
#By year and if the station is in the ostergotland list.
station_precipitation = station_precipitation.filter(lambda x: int(x[0][0])>=1993 and int(x[0][0])<=2016 and x[0][2] in ostergotland_broadcasted.value)

#Get readings
#Get total precipitation per station
average_precipitation_ostergotland_stations = station_precipitation.reduceByKey(lambda a,b: a+b)
    
#Delete the station in key
average_monthly_ostergotland = average_precipitation_ostergotland_stations.map(lambda x: ((x[0][0],x[0][1]), x[1]))
    
#Get the average of each station
average_precipitation_monthly = average_monthly_ostergotland.mapValues(lambda value: (value, 1)).reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])).mapValues(lambda value: value[0]/value[1])
    
# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
average_precipitation_monthly.saveAsTextFile("BDA/output5")


#Output
#This is a sample of the output.
""" ((u'2012', u'09'), 72.75000000000001)
((u'1995', u'05'), 26.00000000000002)
((u'1996', u'12'), 39.55000000000003)
((u'2011', u'08'), 86.26666666666665)
((u'2007', u'04'), 21.249999999999996)
((u'2007', u'06'), 108.94999999999999)
((u'1993', u'04'), 0.0)
((u'2011', u'10'), 43.75000000000001)
((u'2014', u'10'), 72.13749999999999)
((u'1996', u'09'), 57.46666666666667)
((u'1995', u'07'), 43.6) """