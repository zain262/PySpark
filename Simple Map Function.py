# Databricks notebook source
from pyspark import SparkConf, SparkContext

# Create a simple mapping function that shows the lenght of each word in the text file provided

conf = SparkConf().setAppName("Mapping")
sc = SparkContext.getOrCreate(conf)

# COMMAND ----------

# Read in the file
rdd = sc.textFile("/FileStore/tables/test.txt")

# COMMAND ----------

# Display the file contents
rdd.collect()

# COMMAND ----------

# Define a function that splits the words based on spaces and then calculates and stores the length of each string

def sizeCounter(x):
    l=x.split(" ")
    lenght = [];
    for s in l:
        lenght.append(len(s))
    
    return lenght

# Map the function
sep = rdd.map(sizeCounter)

# COMMAND ----------

# Final output
sep.collect()

# COMMAND ----------


