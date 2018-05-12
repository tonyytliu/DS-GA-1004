# DS-GA-1004 Project 

## Topic: Discover Null and Outliers

We improve both K-means Clustering and Attribute Value Frequency algorithms to detect null and outliers in any kinds of large-scale dataset environments. 

### Team: Triple Y

### Members:

Yitao Liu(yl3438) \
Yulin Shen(ys2542) \
Yiyang Sun(ys2380)

## Prerequisites

Spark 2.2.0 \
python 3.4.4

## Datasets

The data are in dumbo, under /scratch/hm74/teaching/big-data/data/GROUP2

## File Description

RunningTime.xlsx: Time comparison between AVF and Kmeans \
a3.py: AVF model with column type detection and data clean \
k9.py: K-means model with column type detection and data clean

## Folder Description

KMeans Result: Outliers results of datasets found by K-means algorithm \
AVF Result: Outliers results of datasets found by AVF algorithm \
TerminalCommand: Command instruction for running the codes 

## Example

### Module Loads:
```bash
$ module load python/gnu/3.4.4
$ module load spark/2.2.0
$ export PYSPARK_PYTHON='/share/apps/python/3.4.4/bin/python'
$ export PYSPARK_DRIVER_PYTHON='/share/apps/python/3.4.4/bin/python'
```

### K-means:

run Kmeans model with parameters number of clusters and outliers percentage \
Submit job example: for dataset 6bic-qvek.tsv, need 4 clusters, 5% of the rows are outliners
```bash
$ spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python k9.py GROUP2/6bic-qvek.tsv 4 5 
```

### AVF:

run AVF model with parameter outliers percentage \
Submit job example: for dataset 6bic-qvek.tsv, 5% of the rows are outliners
```bash
$ spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/6bic-qvek.tsv 5  
```

### Result:

get and merge the output files in Hadoop\
Kmeans result files end with ".out"
```bash
$ hfs -getmerge GROUP2/6bic-qvek.tsv.out 6bic-qvek.tsv.out  
```
AVF result files end with ".avfout"
```bash
$ hfs -getmerge GROUP2/6bic-qvek.tsv.avfout 6bic-qvek.tsv.avfout  
```

