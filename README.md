# DS-GA-1004 Project 

## Topic: Discover Null and Outliers

## Team: Triple Y

## Members:

Yitao Liu(yl3438) \
Yulin Shen(ys2542) \
Yiyang Sun(ys2380)


We use 3 different layers pretrained resnet CNN models with 3 different kinds of RNN(Elman, GRU, LSTM) to get 9 combinations. We compare their results by bleu scores.

## Team members:

Yulin Shen(ys2542) \
Shenghui Zhou(sz2396) \
Yiyan Chen(yc2462)

## Prerequisites

Python 3.6 for Model \
Python 2.7 for Evaluation(COCOAPI use version2.7) \
Pytorch

## Folder Description

Model: all python files to do image caption \
Evaluation: evaluate the result produced by Model to get bleu scores \
Score: 9 combinations of models bleu scores outputs \
Result: predicted captions of 9 combinations of models using val image set \
Train: loss and perplexity in the train process in 9 combinations of models

## Getting Started

### In Model folder: 

Step 1: get COCO Dataset at first
```bash
$ ./data.sh   
```
Step 2: get annation wrapper pickle file
```bash
$ python build_vocab.py 
```
Step 3: resize all images in train image set
```bash
$ python resize.py 
```
Step 4: train the model
```bash
$ python train.py 
```
Step 5: get predicted result of val image set
```bash
$ python sample.py 
```
Notice: change the paths(model setting pickle file, annation pickle file, image sets folder) in each file 

### In Evaluation foler: 

Step 1: get COCOAPI
```bash
$ ./COCOAPI.sh 
```
Step 2: convert sort_caption.txt into a new annotation json to fit our evaluation format 
```bash
$ python create_json_references.py -i ./sort_caption.txt -o ./sort_caption.json 
```
Step 3: choose a result txt file in Result folder to get its bleu score
```bash
$ python run_evaluations.py -i ../Result/LSTM152_Result.txt -r ./sort_caption.json
```
Notice: 

Thanks for [vsubhashin](https://github.com/vsubhashini/caption-eval) sharing us a general evaulation tools. We just need to convert the old annotation json file into a new txt file with image name and its caption.

I have already extracted all necessray information in the val annotation json file to a new txt file called sort_caption.txt, if you need to know how to convert it, please check convert.py in the Evaluation folder for reference.






input:  
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python [project_test#.py] [GROUP2/tsv_file.tsv] [key_cols kmeans_cols] [number_of_clusters] [percentage_of_abnormal_data]


for example:  
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python project_test0.py GROUP2/6bic-qvek.tsv 45 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44 4 10

for 6bic-qvek.tsv file, set col #45 as key, other cols are data used in K-means clustering, use 4 clusters, and 10% of the total data are abnormal data 


.out files for preliminary result are project_test0_0.out.txt, project_test0_1.out.txt, project_test1.out.txt
