from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext, rdd

def delUser(line):
	del line[0]
	return line

conf = SparkConf().setAppName("PythonGeo").setMaster("local[4]")
sc = SparkContext(conf=conf)
text_file = sc.textFile("/home/louis/spark/onlyBayLocsUsersWithFreq.txt")
text_file = text_file.map(lambda line : line.split(" "))
text_file = text_file.flatMap(lambda line : delUser(line))
text_file = text_file.map(lambda line : line.split(","))
text_file = text_file.map(lambda line : (line[0], 1)) \
		.reduceByKey(lambda a,b : a+b).sortBy(lambda a :a[1], ascending= False)
text_file.coalesce(1).saveAsTextFile('/home/louis/spark/outputQ2')
