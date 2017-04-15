# BigDataProject
This repo is for our Big Data Course Project.
Goals: Explore NYC Incidents Data set

This dataset, available on NYC Open Data website, includes all valid felony, misdemeanor, and violation crimes reported to the New York City Police Department (NYPD) from 2006 to the end of last year (2015).

## Run our code 
All code shown in this repo has been tested running on NYU HPC Cluster dumbo. 
### 1. Setting up
After logging into dumbo, you should run the following lines to set up environment:
```bash
module load python/gnu/3.4.4 
export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python 
export PYTHONHASHSEED=0 
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0
```
The dataset should be uploaded to the hdfs.
### 2. Generating labels:
To help us with summarizing the data quality, we can run the following command to automatically generate labels for each column with column index i(beginning from 0):
```bash
spark-submit coli.py [PATH_TO_THE_DATASET]
```
For example, one can generate labels for the first column by running:
```bash
spark-submit col0.py [PATH_TO_THE_DATASET]
```
This will generate test.out file as output on HDFS.
### 3. Cleaning the data:
One can use our script to conduct the data clenaing procedure:
```bash
spark-submit data_clean.py [PATH_TO_THE_DATASET]
```
This will save the cleaned data on HDFS as cleaned_data.out.

