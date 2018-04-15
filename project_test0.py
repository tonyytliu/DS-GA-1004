#!/usr/bin/env python

import sys
from pyspark import SparkContext
from csv import reader

import numpy as np
import math
from operator import add
from pyspark.mllib.clustering import KMeans, KMeansModel

#project_test0.py /GROUP2/file_name.tsv 
#[2]Key_Cols e.g.: 0,1
#[3]KMeans_Cols e.g.: 2,3
#[4]Cluster_nbr e.g.: 3
#[5] Perc_nbr% e.g.: 10 (means 10%)

def gen_lst(x):
    re = []
    for i in KMeans_Cols:
        try:
            re.append(int(x[i]))
        except:
            re.append(float(x[i]))
    return re


def addclustercols(x):
    point = np.array(gen_lst(x)) #for points used in Kmeans
    key = []
    for i in Key_Cols:
        key.append(x[i])
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
    return (key, int(cl), mindist)
        

if __name__ == "__main__":   

    #dealing with input
    sc = SparkContext()
    tsvf = sc.textFile(sys.argv[1], 1)
    header = tsvf.first() #header of the tsv file
    tsvf = tsvf.filter(lambda row: row != header) #tsv file without header
    tsv_rdd = tsvf.map(lambda line: line.split('\t')) #tsv data as rdd
    
    Key_Cols = [] #which col represents keys
    KMeans_Cols = [] #which col needed to train in kmeans
    
    try:
        Key_Cols = sys.argv[2].split(",")
        Key_Cols = [int(k) for k in Key_Cols]
    except:
        Key_Cols = [0]
        print("Inccorect input, set default col_nbr=0")
    
    try:
        KMeans_Cols = sys.argv[3].split(",")
        KMeans_Cols = [int(m) for m in KMeans_Cols]
    except:
        KMeans_Cols = [0]
        print("Inccorect input, set default col_nbr=0")
    
    k_cl = 3 #defalut
    try:
        k_cl_in = int(sys.argv[4])
    except:
        k_cl_in = 0
        
    if k_cl_in > 0:
        k_cl = k_cl_in
    else:
        print("Inccorect input, set default k=3")
    
    total_count = tsv_rdd.count()
    try:
        perc_out = float(sys.argv[5])/100
    except:
        perc_out = 2
    nbr_out = 1 #defalut
    if perc_out <= 1 and perc_out > 0:
        nbr_out = int(int(total_count) * perc_out)
    else:
        nbr_out = int(int(total_count) * 0.1)
        print("Inccorect input, set default perc_out=10%")
    
    #Kmeans and find outliers
    
    a = tsv_rdd.map(lambda x: np.array(gen_lst(x)))
    clusters = KMeans.train(a, k_cl, maxIterations=10, initializationMode="random")
    
    rdd_w_clusts = tsv_rdd.map(lambda x: np.array(addclustercols(x)))
    
    sel_outlier = rdd_w_clusts.map(lambda x: (x[2],(x[0],x[1])))\
                              .sortByKey(False)\
                              .take(nbr_out)
    
    
    out = sc.parallelize(sel_outlier)\
                .map(lambda x:'{0:s}, {1:d}, {2:f}'.format(str(x[1][0]),int(x[1][1]),float(x[0])))
    out.saveAsTextFile("project_test0.out")
    sc.stop()