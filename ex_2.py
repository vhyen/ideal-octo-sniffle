from pyspark.sql import SparkSession
from pyspark import SparkConf
import configparser
import os
from datetime import datetime
from collections import namedtuple

def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf

S3_BUCKET_NAME = "fsa-de"
S3_FILE_KEY = "data_100mil.txt"

if __name__ == '__main__':
    conf = get_spark_app_config()

    start_time = datetime.now()
    spark = SparkSession.builder\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2") \
        .config("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("com.amazonaws.services.s3.enableV4", "true") \
        .config("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config(conf=conf) \
        .getOrCreate()
    print(f"Spark session started in: {(datetime.now() - start_time).total_seconds()} seconds")

    start_time = datetime.now()
    s3_path = f"s3a://{S3_BUCKET_NAME}/{S3_FILE_KEY}"
    rdd = spark.sparkContext.textFile(s3_path)
    rdd_partitioned = rdd.repartition(6)
    rdd = rdd.map(lambda x: int(x) if x.isdigit() else None).filter(lambda x: x is not None) 
    new_rdd = rdd_partitioned.map(lambda x: x.split(" ")).map(lambda x: (int(x[0]), int(x[1])))
    print(f"RDD created in: {(datetime.now() - start_time).total_seconds()} seconds")


    start_time = datetime.now()
    pair = namedtuple('Pair', ['key', 'values'])
    new_rdd = new_rdd.map(lambda x: pair(x[0], x[1]))
    averages = new_rdd.groupByKey().mapValues(lambda values: sum(values) / len(values))
    
    # sum_and_count = new_rdd.mapValues(lambda v: (v, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    # averages = sum_and_count.mapValues(lambda x: round(x[0] / x[1], 2))


    results = averages.collect()
    print(f"Average values calculated in: {(datetime.now() - start_time).total_seconds()} seconds")
    # timestamp = datetime.now().strftime('%m:%d:%H:%M')
    timestamp = datetime.now().strftime('%H:%M_%m-%d')

    output_path = os.path.join(os.getcwd(), 'average_values', f"{rdd.count()}_num_{timestamp}.txt")
    try:
        start_time = datetime.now()
        with open(output_path, 'w') as f:
            f.write('\n'.join(map(str, results)))
        print(f"Prime numbers saved to: {output_path}")
        print(f"Prime numbers saved in: {(datetime.now() - start_time).total_seconds()} seconds")
    except Exception as e:
        print(f"Error saving file: {str(e)}")
    finally:
        spark.stop()