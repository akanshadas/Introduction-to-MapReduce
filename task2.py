from pyspark import SparkContext, StorageLevel
from collections import defaultdict
import json
import os
import time
import sys


#COMMANDLINE ARGUMENTS
review_file = sys.argv[1]
business_file = sys.argv[2]
output_file = sys.argv[3]
if_spark = sys.argv[4]
n =  int(sys.argv[5]);

start = time.time()
solution = dict()

if (if_spark == "no_spark"):

    print("no spark")

    with open(business_file,'r') as business_file:
        business_data = business_file.readlines()
    with open(review_file,'r') as review_file:
        review_data = review_file.readlines()

    list_of_categories = []
    for data in business_data:
           b_data = json.loads(data)

           if b_data['categories'] is not None:
               for b in b_data['categories'].split(', '):
                   list_of_categories.append((b_data['business_id'], b.strip()))

    list_of_stars = []
    for data in review_data:
        r_data = json.loads(data)
        list_of_stars.append((r_data['business_id'].strip(),r_data['stars']))

    stars_hash = defaultdict(list)
    categories_hash = defaultdict(list)
    for bid, stars in list_of_stars:
        stars_hash[bid].append(stars)

    for bid, cat in list_of_categories:
        if bid in stars_hash:
            for s in stars_hash[bid]:
                categories_hash[cat].append(s)

    avg_stars = dict()
    for key,value in categories_hash.items():
        avg_stars[key]=sum(value)/float(len(value))

    sol = []
    sol = sorted(avg_stars.items(), key=lambda e: e[0])
    final_answer  = sorted(sol, key=lambda e: e[1], reverse=True)

    la = []
    for i in range(n):
        la.append(final_answer[i])
    solution["result"]= la
    print ("A:", solution)

    
    
else:

    SparkContext.setSystemProperty('spark.executor.memory', '4g')
    SparkContext.setSystemProperty('spark.driver.memory', '4g')
    sc = SparkContext('local[*]', 'task2')


    reviewsRDD = sc.textFile(review_file).map(lambda x: json.loads(x)).map(lambda entry: (entry['business_id'], entry['stars'])).persist(storageLevel=StorageLevel.MEMORY_ONLY)
    businessRDD = sc.textFile(business_file).map(lambda x: json.loads(x)).map(lambda entry: (entry['business_id'], entry['categories'])).persist(storageLevel=StorageLevel.MEMORY_ONLY)

    def removeSpacesAndCommas(s):
        arr = []
        if s is None:
            return arr
        if (", " in s):
            s = s.replace(", ", ",")
            arr = s.split(",")
        else:
            arr.append(s)
        return arr

    def average(arr):
        sum = 0
        for a in arr:
            sum += a
        return sum/float(len(arr))



    avg_stars = businessRDD.join(reviewsRDD)\
        .map(lambda e: (removeSpacesAndCommas(e[1][0]), e[1][1]))\
        .flatMap(lambda e: [(ele, e[1]) for ele in e[0]])\
        .groupByKey().mapValues(list)\
        .map(lambda e: (e[0], average(e[1]))).sortBy(lambda x: (-x[1], x[0])).take(n)

    solution["result"] = avg_stars


task2_output_file = open(output_file, "w")
task2_output_file.write(json.dumps(solution))
task2_output_file.close()

end = time.time()