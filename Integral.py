from __future__ import division
import sys
from random import random
from operator import add
import math
import time
import csv

f = lambda x : 1/x

from pyspark.sql import SparkSession


if __name__ == "__main__":

    spark = SparkSession\
    .builder\
    .appName("PythonIntegral")\
    .getOrCreate()

    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2

    a = 1
    b = 10
    n = 1000
    I = 0
    batch_size_for_mean = 10
    p = 1.0*(b-a)/n
    sum = 0
    values = []
    time_values = []
    meantime = 0

    values.append('val')
    time_values.append('time')
    def integrale(index):
        return f(a+index*p)

    for i in [1,10,50,100,1000,10000,100000]:
        def integrale(index):
            return f(a+index*(1.0*(b-a)/i))
        for j in range(0, batch_size_for_mean):
            starttime = time.time()
            I = spark.sparkContext.parallelize(range(a, i+1), partitions).map(integrale).reduce(add)
            stoptime = time.time()
	    meantime = meantime + (stoptime - starttime)
            sum = sum + I*(1.0*(b-a)/i)

        values.append(sum/10)
        time_values.append(meantime/10)
        sum = 0
        meantime = 0
    spark.stop()

    with open('/home/louis/spark/outputIntegral.csv', 'wb') as f:
        writer = csv.writer(f)
        writer.writerow(values)
        writer.writerow(time_values)
