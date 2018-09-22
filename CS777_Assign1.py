# cs777 assignment 1

from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext
sc = SparkContext()


if __name__ == "__main__":



    # Task 1: the top ten taxis that have had the largest number of drivers

    lines = sc.textFile("file:///Users/fanggeyou/Desktop/taxi-data-sorted-small.csv")
    #lines.take(1)
    #text_file = sc.textFile("s3://metcs777/taxi-data-sorted-small.csv.bz2")

    taxis = lines.map(lambda x: x.split(','))
    taxisList = taxis.filter(lambda x: x != "").map(lambda x: (x[0], 1)).reduceByKey(add) #clean out missing value
    #taxisList.take(1)

    taxisList.collect().top(10)


    #Task 2: the top 10 best drivers in terms of average earned money per minute spent
    drivers1 = taxis.map(lambda x: (x[1], float(x[16]))).reduceByKey(lambda x, y: x + y) #total amount

    drivers2 = taxis.map(lambda x: (x[1], int(x[4]))).reduceByKey(lambda x, y: x + y) #trip time
    dr = drivers1.join(drivers2)
    #dr.take(1)
    result = dr.map(lambda (x, (a, b)): (x, (a/b*60)))
    #sc.parallelize(list(result)).sortBy(lambda x: x[1]).top(10)
    result.top(10, lambda x: x[1])

    #Task 3: find the hour with largest profit ratio
    hour1 = taxis.map(lambda x: (x[2], float(x[12]))).reduceByKey(lambda x, y: x + y) #surcharge total

    hour2 = taxis.map(lambda x: (x[2], float(x[5]))).reduceByKey(lambda x, y: x + y) #distance total

    hours = hour1.join(hour2)
    #hours.take(1)
    profit = dr.map(lambda (x, (a, b)): (x, (a / b)))
    profit.top(1)


    #counts.saveAsTextFile(sys.argv[2])

    #output = counts.collect()

    sc.stop()


