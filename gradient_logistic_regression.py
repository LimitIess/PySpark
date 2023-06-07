from pyspark import SparkContext

sc = SparkContext(appName="exercise")

data = sc.textFile("Iris.csv")
lines = data.map(lambda line: line.split(","))


beta_0 = 1.0
beta_1 = 1.0
converge = 100.0
alpha = 0.01

#Map data into key_value pairs.
data = lines.map(lambda x: ("1", (float(x[1]),float(x[2])))).cache()

while converge > 0.01:
	y_error = data.mapValues(lambda x: ((x[1] - (beta_0 + beta_1 * x[0])),
			(x[1] - ((beta_0 + beta_1 * x[0])* x[0])), (x[1] -(beta_0 + beta_1 * x[0])**2)))
	sum_y_error = y_error.reduceByKey(lambda a,b: (a[0] + b[0], a[1]+b[1], a[2]+b[2])).values().collect()
	beta_0 = beta_0 - alpha * sum_y_error[0][0]
	beta_1 = beta_1 - alpha * sum_y_error[0][1]
	#converge = [alpha * float(element) for element in sum_y_error[0][0:2]]
	converge = (1/2)*(sum_y_error[0][2])

#print(converge)
#print(beta_0)
#print(beta_1)