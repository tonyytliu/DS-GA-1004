#!/usr/bin/env python

#AVF with String

import sys
from pyspark import SparkContext
from csv import reader

import numpy as np
import math
from operator import add

#[0]xxxx.py
#[1]GROUP2/file_name.tsv 
#[2] Perc_nbr% e.g.: 10 (means 10%)

def new_coldict(d_i, e, tol):
    #cal nbr of same entries
    str_reduce = tsv_rdd.map(lambda x: (x[e],1))\
                        .reduceByKey(add)
    new_dict = str_reduce.collect()
    for element in new_dict:
        #add each unique entry to dict, with value of its freq
        dict_lst[d_i][element[0]]=element[1]/tol
    return

def string_freq(x):
    for e in AVF_Cols:
        #in dict: {x[e]:freq of x[e]}
        x[e] = str(dict_lst[ind_dict[e]][x[e]])
    return x

def gen_lst(x):
    re = []
    for i in AVF_Cols:
        try:
            re.append(int(x[i]))
        except:
            try:
                re.append(float(x[i]))
            except:
                re.append(-999)
    return re

def avf_score(x):
    avf_score = 0.0
    l = gen_lst(x)
    key = x[-1]
    for i in range(len(l)):
        avf_score += l[i]
    return (key, avf_score)

def add_id(x):
    re = []
    for i in range(len(x[0])):
        re.append(x[0][i])
    re.append(x[1])
    return re

def find_null(x):
    col_ind = x[-1]
    count = 0
    for i in range(len(x)-1):
        if x[i] == "NA" or x[i] == "":
            count += 1
    if count >= len(x)/2:
        x[-1] = (col_ind,-1)
    return x

def find_dirty(x):
    col_ind = x[-1]
    for i in Num_Cols:
        try:
            float(x[i])
        except:
            x[-1] = (col_ind,-1)
    return x

def bucket_value(x):
    if current_bucket == 0:
        x[current_col] = "-1"
    else:
        tempval = (float(x[k]) - bucket_min)//current_bucket
        x[current_col] = str(tempval)
    return x

if __name__ == "__main__":   
    
    #dealing with input
    sc = SparkContext()
    file_name = sys.argv[1]
    tsvf = sc.textFile(sys.argv[1], 1)
    header = tsvf.first() #header of the tsv file
    tsvf = tsvf.filter(lambda row: row != header) #tsv file without header
    tsv_rdd = tsvf.map(lambda line: line.split('\t')) #tsv data as rdd
    tsv_rdd = tsv_rdd.zipWithIndex().map(lambda x: add_id(x))
    
    Key_Col = len(tsv_rdd.first())-1 #which col represents keys
    AVF_Cols = [] #which col needed to train in kmeans
    Num_Cols = []
    Cat_Cols = []
    
    tsv_rdd = tsv_rdd.map(lambda x: find_null(x))
    null_row = tsv_rdd.filter(lambda x: type(x[-1]) != int).map(lambda x: str(x[-1][0]))
    tsv_rdd = tsv_rdd.filter(lambda x: type(x[-1]) == int)
        
    total_count = tsv_rdd.count()
    test_count = int(0.05*total_count) + 1
    for n in range(len(tsv_rdd.first())-1):
        num_check = 0
        test_col = tsv_rdd.map(lambda x:x[n])
        test_col_take = test_col.take(test_count)
        #num_uniq = len(np.unique(test_col))
        num_uniq = test_col.distinct().count()
        if num_uniq < 51:
            Cat_Cols.append(n)
        else:
            for m in range(test_count):
                try:
                    float(test_col_take[m])
                except:
                    num_check = num_check + 1
            if num_check < 0.01*total_count:
                Num_Cols.append(n)
    
    tsv_rdd = tsv_rdd.map(lambda x: find_dirty(x))
    dirty_row = tsv_rdd.filter(lambda x: type(x[-1]) != int).map(lambda x: str(x[-1][0]))
    tsv_rdd = tsv_rdd.filter(lambda x: type(x[-1]) == int)

    current_col = 0
    current_bucket = 0
    bucket_min = 0
    for k in Num_Cols:
        test_col = tsv_rdd.map(lambda x: float(x[k]))
        tsv_rdd.map(lambda x: float(x[k])).max()
        diff = float(test_col.max()) - float(test_col.min())
        bucket = diff/50
        current_col = k
        current_bucket = bucket
        bucket_min = float(test_col.min())
        tsv_rdd = tsv_rdd.map(lambda x: bucket_value(x))    

    
    AVF_Cols = list(set(Num_Cols).union(Cat_Cols))
    total_count = tsv_rdd.count()

    try:
        perc_out = float(sys.argv[2])/100
    except:
        perc_out = 2
    nbr_out = 1 #defalut
    if perc_out <= 1 and perc_out > 0:
        nbr_out = int(int(total_count) * perc_out)
    else:
        nbr_out = int(int(total_count) * 0.1)
        print("Inccorect input, set default perc_out=10%")
    
    #String to freq
    
    total_lines = tsv_rdd.count()
    #dict_lst=[dict1_for_col1, dict2_for_col2, ...]
    dict_lst = []
    for i in AVF_Cols:
        dict_lst.append({})
    #ind_dict = {col_nbr:position_in_dict_list}
    ind_dict = {}
    dict_i = 0
    for e in AVF_Cols:
        new_coldict(dict_i, e, total_lines) #add entries in cols to dicts
        ind_dict[e] = dict_i #record col_nbr and dict_lst position
        dict_i += 1
    tsv_rdd = tsv_rdd.map(lambda x: string_freq(x)) #change string to frequent(float)
    
    #AVF
       
    score = tsv_rdd.map(lambda x: avf_score(x))
    
    score_out = score.map(lambda x: (x[1], x[0]))\
                     .sortByKey()\
                     .take(nbr_out)
    
    
    out1 = sc.parallelize(score_out)\
                .map(lambda x:'{0:s}, {1:f}'.format(str(x[1]),float(x[0])))
    out0 = sc.parallelize(["Num Cols: " + str(Num_Cols) + \
                           "\nCat Cols: "+ str(Cat_Cols) + \
                           "\nNull Rows: " + str(null_row.collect()) + \
                           "\nDirty Rows: " + str(dirty_row.collect())])
    out = out0.union(out1)
    out.saveAsTextFile(file_name + ".avfout")
    sc.stop() 
