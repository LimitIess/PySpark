from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree, RandomForest

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km

def calculate_time_diff(time, values):
    """Calculate difference in hours
    times: Hour to calculate
    values: Hours when the calculation were made.
    """
    time_diff = abs(int(values[1])-int(time[0:2]))
    if time_diff > 12:
       time_diff = 24-time_diff
    return time_diff

def kernelize(diff_values, kernel_width):
    """Gaussian kernel trick
    diff_values: Differences betweeen point of interest and each data point
    kernel_width: Width of the kernel
    """
    return exp((-diff_values**2)/(2*(kernel_width**2)))


sc = SparkContext(appName="exercise")
#Initializing width, prediction point and date
h_distance = 40# Up to you
h_date = 730# Up to you
h_time = 4# Up to you
a = 60.619 # Up to you
b = 15.6603 # Up to you
date = "2007-10-08" # Up to you

#Read in data
stations = sc.textFile("BDA/input/stations.csv")
temps = sc.textFile("BDA/input/temperature-readings.csv")

#Used for sampling smaller dataset
#temps = temps.sample(False, 0.1)

# Your code here
#Split the file
lines = stations.map(lambda line: line.split(";"))
# (key, value) = (station_number, (Latitude, longtiude))
stations_coordinates = lines.map(lambda x: (x[0], (float(x[3]), float(x[4]))))

#Split the file
lines_temp = temps.map(lambda line: line.split(";"))

#Code will differ from this part between gaussian kernels and mllib models! As the code was run in the chunks
#in order to get within the time limit.

#Additive/Multiplicative Gaussian kernels

#kernel 1
# - Distance between stations from already defined kernelize function --> We need station number, longitude and latitude
kernel_1 = stations_coordinates.mapValues(lambda x: kernelize(haversine(lon1= x[1],
                                                              lat1= x[0],
                                                              lat2= a,
                                                              lon2= b
                                                              ), h_distance))

#Collect as it map and broadcast to join it to the temperature
rdd_coordinates = kernel_1.collectAsMap()
bc = sc.broadcast(rdd_coordinates)


#Map (key, value) = (station_number, (date, time, temp, kernel1))
stations_temperatures = lines_temp.map(lambda x: (x[0], (datetime.strptime(x[1], "%Y-%m-%d"), x[2][0:2], float(x[3]), bc.value[x[0]])))

#Create empty lists for predictions for additive and multiplicative
add_predictions = list()
mult_predictions = list()


#Filter data with cache to store it in the memory as it will be reused
stations_temperatures = stations_temperatures.filter(lambda x: x[1][0] <= datetime.strptime(date, "%Y-%m-%d")).cache()

#For loop for each time
for time in ["24:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00", "12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]:

    #Now that we know that we have data for the same day or earlier. If we are using or ("|") logical operatior. The first statement will 
    # allow all the prediction for the same day but hours before the prediction.
    #The second statement will allow all previous days observations.
    stations_time_diff = stations_temperatures.filter(lambda x: (x[1][1] < time[0:2]) | (x[1][0] < datetime.strptime(date, "%Y-%m-%d")))

    #convert it to (key, value) --> (station, (Temperature, kernel 1, kernel2, kernel3)    
    tmp_kernel = stations_time_diff.mapValues(lambda x: (x[2],
                                                            kernelize(calculate_time_diff(time, x), h_time),
                                                            kernelize((datetime.strptime(date, "%Y-%m-%d")-x[0]).days%365, h_date),
                                                            kernelize(x[3],h_distance)))

    #To make the prediction
    #Delete stations and store it as (temperature*(additive kernels), (additive kernels)
    #Sum them up 
    add_kernels =  tmp_kernel.map(lambda x: (x[1][0]*(x[1][1]+x[1][2]+x[1][3]), (x[1][1]+x[1][2]+x[1][3]))).reduce(lambda a,b: (a[0]+b[0], a[1]+b[1]))
    #Divide key with value
    add_predictions.append(add_kernels[0]/add_kernels[1])

    #Same steps are done for multiplicative, but instead multiplicative kernel is used.
    #Multiplicative
    mult_kernels =  tmp_kernel.map(lambda x: (x[1][0]*(x[1][1]*x[1][2]*x[1][3]), (x[1][1]*x[1][2]*x[1][3]))).reduce(lambda a,b: (a[0]+b[0], a[1]+b[1]))
    mult_predictions.append(mult_kernels[0]/mult_kernels[1])


#Convert both to rdd in order to use saveAsTextFile
final_mult_predictions = sc.parallelize(mult_predictions)
final_mult_predictions.saveAsTextFile("BDA/output/multiplicative")

final_add_predictions = sc.parallelize(add_predictions)
final_add_predictions.saveAsTextFile("BDA3/output/additive")

#Results from code excecution - Additive

"""
    24                22               20             18             16              14
4.40875407707   4.52528725758   4.67207886902   4.83733435365   4.96306438889   5.02135065993

    12                10               08             06             04
5.00211772346   4.87493293011   4.68148049395   4.50295423727   4.38308204658
"""

##Results from code excecution - Multiplicative

"""
    24                22               20             18             16              14
4.37691728086   4.80294411721   5.43647962547   6.10422967881   6.65420184624   6.93197629167   

    12                10               08             06             04
6.86511717505   6.41304725346   5.74070129375   5.04004679560   4.51015612845
"""



#Mllib Models

#MLlib 1 - Decision trees
#Now instead of calculating distance, we are using the coordinates, which are then broadcasted and joined to the temperatures
mllib_rdd_coordinates = stations_coordinates.collectAsMap()
mllib_bc = sc.broadcast(mllib_rdd_coordinates)

#Map it to (key, value) --> (station, (year, time, temperature, coordinates))
mllib_station_temperatures = lines_temp.map(lambda x: (x[0], (datetime.strptime(x[1], "%Y-%m-%d"), x[2][0:2], float(x[3]), mllib_bc.value[x[0]])))
#Filter by year
stations_temperatures = mllib_station_temperatures.filter(lambda x: x[1][0] <= datetime.strptime(date, "%Y-%m-%d")).cache()

#Empty results list
results = list()

#For loop to make prediction for each time.
for time in ["24:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00", "12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]:

    #Now that we know that we have data for the same day or earlier. If we are using or ("|") logical operatior. The first statement will 
    # allow all the prediction for the same day but hours before the prediction.
    #The second statement will allow all previous days observations.
    stations_time_diff = stations_temperatures.filter(lambda x: (x[1][1] < time[0:2]) | (x[1][0] < datetime.strptime(date, "%Y-%m-%d")))

    #Create a new rdd with labeled point for the mllib
    #Form of (temperature, (year, month, day, time, latitude, longtiude))
    mllib_station_temp = stations_time_diff.map(lambda x: LabeledPoint(label = x[1][2], features =
                                                                                  (x[1][0].year,
                                                                                   x[1][0].month,
                                                                                   x[1][0].day,
                                                                                   x[1][1],
                                                                                   x[1][3][0],
                                                                                   x[1][3][1])))

    #Train decision trees model
    model = DecisionTree.trainRegressor(data = mllib_station_temp, categoricalFeaturesInfo = {}, maxDepth=8)



    #List with all prediction parameters
    predict_time = [(datetime.strptime(date, "%Y-%m-%d").year,
                    datetime.strptime(date, "%Y-%m-%d").month,
                    datetime.strptime(date, "%Y-%m-%d").day,
                    time[0:2],
                    a,
                    b)]
    #Create a new rdd with name rdd_prediction of the list with all prediction parameters
    rdd_prediction = sc.parallelize(predict_time)

    #Predict the model, collect the results as list and append it in results
    results.append(model.predict(rdd_prediction).collect())

#Make rdd of results in order to use saveAsTextFile
results_pred = sc.parallelize(results)

#Save the results
results_pred.saveAsTextFile("BDA/output/decision_trees")


#Results from code excecution - MLlib 1 - Decision trees

"""
        24                      22                      20                     18                   16                    14
8.600562057069893      8.600562057069893       8.600562057069893      8.600562057069893     10.725188909508288  10.725188909508288 

        12                      10                      08                     06                     04
10.725188909508288     10.725188909508288      10.725188909508288      6.576891371034673    6.576891371034673
"""

#MLlib 2 - Random Forest

#Now instead of calculating distance, we are using the coordinates, which are then broadcasted and joined to the temperatures
mllib_rdd_coordinates = stations_coordinates.collectAsMap()
mllib_bc = sc.broadcast(mllib_rdd_coordinates)

#Map it to (key, value) --> (station, (year, time, temperature, coordinates))
mllib_station_temperatures = lines_temp.map(lambda x: (x[0], (datetime.strptime(x[1], "%Y-%m-%d"), x[2][0:2], float(x[3]), mllib_bc.value[x[0]])))

#Filter by year
stations_temperatures = mllib_station_temperatures.filter(lambda x: x[1][0] <= datetime.strptime(date, "%Y-%m-%d")).cache()

#Empty results list
results2 = list()

#For loop to make prediction for each time.
for time in ["24:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00", "12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]:

    #Now that we know that we have data for the same day or earlier. If we are using or ("|") logical operatior. The first statement will 
    # allow all the prediction for the same day but hours before the prediction.
    #The second statement will allow all previous days observations.
    stations_time_diff = stations_temperatures.filter(lambda x: (x[1][1] < time[0:2]) | (x[1][0] < datetime.strptime(date, "%Y-%m-%d")))


    #Create a new rdd with labeled point for the mllib
    # Form of (temperature, (year, month, day, time, latitude, longtiude))
    mllib_station_temp = stations_time_diff.map(lambda x: LabeledPoint(label = x[1][2], features =
                                                                                  (x[1][0].year,
                                                                                   x[1][0].month,
                                                                                   x[1][0].day,
                                                                                   x[1][1],
                                                                                   x[1][3][0],
                                                                                   x[1][3][1])))
    #Train RandomForest model
    model2 = RandomForest.trainRegressor(data = mllib_station_temp, categoricalFeaturesInfo = {}, numTrees=8, maxDepth=6)

    
    #List with all prediction parameters
    predict_time = [(datetime.strptime(date, "%Y-%m-%d").year,
                    datetime.strptime(date, "%Y-%m-%d").month,
                    datetime.strptime(date, "%Y-%m-%d").day,
                    time[0:2],
                    a,
                    b)]
    #Create a new rdd with name rdd_prediction of the list with all prediction parameters
    rdd_prediction = sc.parallelize(predict_time)

    #Predict the model, collect the results as list and append it in results
    results2.append(model2.predict(rdd_prediction).collect())

#Make rdd of results in order to use saveAsTextFile
results_pred_2 = sc.parallelize(results2)

#Save the results
results_pred_2.saveAsTextFile("BDA/output/randomforest")

#Results from code excecution - MLlib 2 - Random forest
"""
        24                     22                      20                     18                   16                    14
7.861582588141666      7.861582588141666       7.861582588141666     8.780920290497881    9.922816345589723   9.922816345589723

        12                     10                      08                     06                   04    
9.922816345589723      9.922816345589723      9.711231205432370     8.149503771126607    7.923058026596670

"""
