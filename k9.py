#!/usr/bin/env python

#KMeans
#Deal with both numerical and categorical

import sys
from pyspark import SparkContext
from csv import reader

import numpy as np
import math
from operator import add
from pyspark.mllib.clustering import KMeans, KMeansModel

from pyspark.mllib.feature import StandardScaler, StandardScalerModel
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.util import MLUtils

#[0]xxxx.py
#[1]file_name.tsv 
#[2]Cluster_nbr e.g.: 3
#[3] Perc_nbr% e.g.: 10 (means 10%)

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
    for e in Cat_Cols:
        #in dict: {x[e]:freq of x[e]}
        x[e] = str(dict_lst[ind_dict[e]][x[e]])
    return x

def gen_lst(x): #generate list with only KMeans_Cols
    re = []
    for i in KMeans_Cols:
        try:
            re.append(int(x[i]))
        except:
            try:
                re.append(float(x[i]))
            except:
                re.append(-999)
    return re

def addclustercols(x):
    point = x[:-1] #for points used in Kmeans
    key = x[-1]
    center = clusters.centers[0]
    cl = 0
    mindist = 0.0
    
    s_y = 0
    for i in range(len(point)):
        s_y += (point[i] - center[i])**2
    mindist = math.sqrt(s_y)
    
    for i in range(1,len(clusters.centers)):
        center = clusters.centers[i]
        s_1 = 0
        for n in range(len(point)):
            s_1 += (point[n] - center[n])**2
        distance = math.sqrt(s_1)
        if distance < mindist:
            cl = i
            mindist = distance
    #clcenter = clusters.centers[cl]
    return (int(key), int(cl), mindist)

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

if __name__ == "__main__":   
    
    #dealing with input
    sc = SparkContext()
    file_name = sys.argv[1]
    tsvf = sc.textFile(sys.argv[1], 1)
    header = tsvf.first() #header of the tsv file
    tsvf = tsvf.filter(lambda row: row != header) #tsv file without header
    tsv_rdd = tsvf.map(lambda line: line.split('\t')) #tsv data as rdd
    tsv_rdd = tsv_rdd.zipWithIndex().map(lambda x: add_id(x))
                      
     
    
    Num_Cols = [] #cols that are numberic data
    Cat_Cols = [] #cols that are catagorical data
    KMeans_Cols = [] #which col needed to train in kmeans



    #filter out null rows
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
            if num_check < 0.2*test_count:
                Num_Cols.append(n)
    
    #filter out dirty rows
    tsv_rdd = tsv_rdd.map(lambda x: find_dirty(x))
    dirty_row = tsv_rdd.filter(lambda x: type(x[-1]) != int).map(lambda x: str(x[-1][0]))
    tsv_rdd = tsv_rdd.filter(lambda x: type(x[-1]) == int)
    
    #form KMeans_Cols list with the union of Num and Cat Cols
    KMeans_Cols = list(set(Num_Cols).union(Cat_Cols))
    total_count = tsv_rdd.count()

    if KMeans_Cols == []: #if empty list
        KMeans_Cols = [0]
    
    k_cl = 3 #defalut
    try:
        #k_cl_in = int(sys.argv[4])
        k_cl_in = int(sys.argv[2]) 
    except:
        k_cl_in = 0
        
    if k_cl_in > 0:
        k_cl = k_cl_in
    else:
        print("Inccorect input, set default k=3")
    
    try:
        #perc_out = float(sys.argv[5])/100
        perc_out = float(sys.argv[3])/100
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
    
    if Cat_Cols != []:
        #dict_lst=[dict1_for_col1, dict2_for_col2, ...]
        dict_lst = []
        for i in Cat_Cols:
            dict_lst.append({})
        #ind_dict = {col_nbr:position_in_dict_list}
        ind_dict = {}
        dict_i = 0
        for e in Cat_Cols:
            new_coldict(dict_i, e, total_lines) #add entries in cols to dicts
            ind_dict[e] = dict_i #record col_nbr and dict_lst position
            dict_i += 1
        tsv_rdd = tsv_rdd.map(lambda x: string_freq(x)) #change string to frequent(float)
        
    #Kmeans Cols
    a = tsv_rdd.map(lambda x: np.array(gen_lst(x)))
    
    #normalization
    scaler1 = StandardScaler().fit(a)
    a = scaler1.transform(a)
    
    #Kmeans
    clusters = KMeans.train(a, k_cl, maxIterations=10, initializationMode="random")
    
    col_ind_rdd = tsv_rdd.map(lambda x: x[-1]) #col_ind col
    a = a.zip(col_ind_rdd).map(lambda x: add_id(x)) #add col_ind col back to KMeans_cols
    rdd_w_clusts = a.map(lambda x: np.array(addclustercols(x)))
    
    sel_outlier = rdd_w_clusts.map(lambda x: (x[2],(x[0],x[1])))\
                              .sortByKey(False)\
                              .take(nbr_out)
    
    out1 = sc.parallelize(sel_outlier)\
                .map(lambda x:'{0:d}, {1:d}, {2:f}'.format(int(x[1][0]),int(x[1][1]),float(x[0])))
    
    out0 = sc.parallelize(["Num Cols: " + str(Num_Cols) + \
                           "\nCat Cols: "+ str(Cat_Cols) + \
                           "\nNull Rows: " + str(null_row.collect()) + \
                           "\nDirty Rows: " + str(dirty_row.collect())])

    out = out0.union(out1)
    out.saveAsTextFile(file_name + ".out")
    sc.stop() 
    
