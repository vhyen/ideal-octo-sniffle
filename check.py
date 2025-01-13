from pyspark.sql import SparkSession
from pyspark import SparkConf
import configparser
import os
from datetime import datetime

def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf

if __name__ == '__main__':
    conf = get_spark_app_config()
    spark = SparkSession.builder\
        .config(conf=conf) \
        .getOrCreate()
    
    rdd = spark.sparkContext.textFile("assignment/ideal-octo-sniffle/data/data_100mil.txt")

    print(rdd.take(10))    

