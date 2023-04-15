from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(appName = "exercise 1")
    # This path is to the file on hdfs
    temperature_file = sc.textFile("temperature-readings-small.csv")
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
    count_temperatures.saveAsTextFile("BDA/output2")

