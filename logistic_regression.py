from pyspark import SparkContext

sc = SparkContext(appName="exercise")

data = sc.textFile("Iris.csv")
lines = data.map(lambda line: line.split(","))

# (key, value) = (1,(width, length))
#For summing up all the values, it is important to give them the same key.
iris_measurements = lines.map(lambda x: ("1", (float(x[1]),float(x[2])))).cache()

#Linear Regression
#Corresponds to number of rows in iris dataset
n = 150

#reduceByKey to get Sum x and sum y --> .values() to get only values --> Collect to get it as list
xy_sum = iris_measurements.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1])).values().collect()
print(xy_sum)
#As it is a list we need first bracket to get into the list [0] and then value y=1 and x=0
#[(876.4999999999998, 458.10000000000014)] 
y_sum = xy_sum[0][1]
x_sum = xy_sum[0][0]
#As it is a python values we can just divide it by n to get the average
y_ave = y_sum/n
x_ave = x_sum/n

#Then we calculate the numerator according to the given formula
numerator = iris_measurements.mapValues(lambda x: (float(x[0])-x_ave)*(float(x[1])-y_ave))
#Sum up all values and collect it as a list
sum_numerator = numerator.reduceByKey(lambda a,b: a+b).values().collect()
#Then we calculate the denominator according to the given formula
denominator = iris_measurements.mapValues(lambda x: (float(x[0])-x_ave)**2)
#Sum up all values and collect it as a list
sum_denominator = denominator.reduceByKey(lambda a,b: a+b).values().collect()

#As the results are in the list and we can therefore, just get the value through indexing 0.
#Compute beta_1 estimator
beta_1 = sum_numerator[0]/sum_denominator[0]

#Compute beta_0 estimator
beta_0 = y_ave - beta_1 * x_ave

print(beta_1)
print(beta_0)
