# DS-GA-1004

input:
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python [project_test#.py] [GROUP2/tsv_file.tsv] [key_cols kmeans_cols] [number_of_clusters] [percentage_of_abnormal_data]

for example:
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python project_test0.py GROUP2/6bic-qvek.tsv 45 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44 4 10
for 6bic-qvek.tsv file, set col #45 as key, other cols are data used in K-means clustering, use 4 clusters, and 10% of the total data are abnormal data 
 