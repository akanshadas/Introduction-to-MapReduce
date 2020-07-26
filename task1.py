from pyspark import SparkContext, StorageLevel
from operator import add
import json
import os
import time
import sys


input_file_path = sys.argv[1]
output_file_path = sys.argv[2]
stopwords_f = sys.argv[3]
y = sys.argv[4]
m = int(sys.argv[5]);
n = int(sys.argv[6]);



#Python: $ spark-submit task1.py <input_file> <output_file> <stopwords> <y> <m> <n>

solution = dict()
solution["A"] = 0
solution["B"] = 0
solution["C"] = 0
solution["D"] = []                
solution["E"] = ["good", "bad"]
                 
task1_output_file = open(output_file_path, "w")
task1_output_file.write(json.dumps(solution))
task1_output_file.close()
                
start = time.time()
SparkContext.setSystemProperty('spark.executor.memory', '4g')
SparkContext.setSystemProperty('spark.driver.memory', '4g')
sc = SparkContext('local[*]', 'task1')

reviewRDD = sc.textFile(input_file_path).map(json.loads).map(lambda entry: ((entry['review_id'], entry['user_id'], entry['business_id'], entry['stars'], entry['text'], entry['date']), 1)).persist(storageLevel=StorageLevel.MEMORY_ONLY)
    
#Q1 The total number of reviews (0.5pts)
total_count = reviewRDD.map(lambda e: e[0][0]).distinct().count()
solution["A"] = total_count 
        
task1_output_file = open(output_file_path, "w")
task1_output_file.write(json.dumps(solution))
task1_output_file.close()
                 
#Q2 The number of reviews in a given year, y (0.5pts)
reviews_2018 = reviewRDD.filter(lambda e: e[0][5][0:4] == y).count()
solution["B"] = reviews_2018

task1_output_file = open(output_file_path, "w")
task1_output_file.write(json.dumps(solution))
task1_output_file.close()
                 
#Q3 The number of distinct users who have written the reviews (0.5pts)
users = reviewRDD.map(lambda e: e[0][1]).distinct().count()
solution["C"] = users
                
task1_output_file = open(output_file_path, "w")
task1_output_file.write(json.dumps(solution))
task1_output_file.close()
                 
                 
#Q4 Top m users who have the largest number of reviews and its count (0.5pts)

top_user = reviewRDD.map(lambda e:(e[0][1], 1)).reduceByKey(lambda valCount, n : valCount + n).sortBy(lambda x: (-x[1], x[0])).take(m)
solution["D"] = top_user

task1_output_file = open(output_file_path, "w")
task1_output_file.write(json.dumps(solution))
task1_output_file.close()


#Q5 Top n frequent words in the review text. The words should be in lower cases.
def removePunctuations(x):
    symbols = ["(", "[", ",", ".", "!", "?", ":", ";", "]", ")"]
    for ch in symbols:
        if (ch in x):
            x = x.replace(ch,'')
    return x


stopwords_list = []
stopwords_file = open(stopwords_f, "r")
for stopWord in stopwords_file:
    l = len(stopWord)
    stopWord = stopWord.rstrip('\n')
    stopwords_list.append(stopWord)
stopwords_list.append("")
stopwords_file.close()
#print ("stopwords_list:",stopwords_list)

words_freq = reviewRDD.flatMap(lambda e: e[0][4].lower().split()).map(lambda e: (removePunctuations(e),1)).filter(lambda e: e[0] not in stopwords_list).reduceByKey(lambda valCount, n : valCount + n).sortBy(lambda x: (-x[1], x[0])).map(lambda x: x[0]).take(n)

solution["E"] = words_freq


end = time.time()

task1_output_file = open(output_file_path, "w")
task1_output_file.write(json.dumps(solution))
task1_output_file.close()

