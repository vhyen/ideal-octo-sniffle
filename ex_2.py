from pyspark.sql import SparkSession
from pyspark import SparkConf
import configparser
import os
import shutil
from datetime import datetime
from collections import namedtuple

def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, val) in config.items("EX_2"):
        spark_conf.set(key, val)
    return spark_conf

if __name__ == '__main__':
    conf = get_spark_app_config()
    # spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark = SparkSession.builder.appName("ex_2").getOrCreate()

    rdd = spark.sparkContext.textFile("data_10.txt")


    new_rdd = rdd.map(lambda x: x.split(" ")).map(lambda x: (int(x[0]), int(x[1])))
    pair = namedtuple('Pair', ['key', 'values'])
    new_rdd = new_rdd.map(lambda x: pair(x[0], x[1]))

    # Pair = namedtuple('Pair', ['key', 'values'])
    # new_rdd = new_rdd.groupByKey().map(lambda x: Pair(x[0], list(x[1])))

    # new_rdd = new_rdd.reduceByKey(lambda x, y: x + y)
    grouped_rdd = new_rdd.groupByKey().mapValues(lambda values: sum(values) / len(values))
    

    print(grouped_rdd.take(10))