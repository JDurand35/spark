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

    
    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2

    # The path for school VM
    PATH = '/home/louis/spark/outputIntegral.csv'

    a = 1
    b = 10
    n = 1000
    I = 0
    batch_size_for_mean = 10
    p = 1.0*(b-a)/n
    sum = 0
    values = []
    time_values = []
    temp_time_values = []
    meantime = 0

    values.append('val')
    time_values.append('time')
    def integrale(index):
        return f(a+index*p)

    for i in [10**q for q in range(5)]:
        
        print('nb of rect', i)

        def integrale(index):
            return f(a+index*(1.0*(b-a)/i))
        
        for workers in [2**k for k in range(6)]:
            # print('    nb of workers', workers)
            spark = SparkSession\
                    .builder\
                    .master("local[" + str(workers)  + "]")\
                    .appName("PythonIntegral")\
                    .getOrCreate()
            
            for j in range(0, batch_size_for_mean):
                starttime = time.time()
                I = spark.sparkContext.parallelize(range(a, i+1), partitions).map(integrale).reduce(add)
                stoptime = time.time()
                meantime = meantime + (stoptime - starttime)
                sum = sum + I*(1.0*(b-a)/i)
            print('    number of workers', workers, ' ::: Meantime (10 batch): ', meantime/10, 'Integral value: ', sum/10)
            temp_time_values.append(str(workers) + ' workers')
            temp_time_values.append(meantime/10)
            sum = 0
            meantime = 0
            spark.stop()
        time_values.append(str(i) + ' rectangles')
        time_values.append(temp_time_values)
        temp_time_values = []


    print(time_values)

    with open('outputIntegral.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(values)
        writer.writerow(time_values)
