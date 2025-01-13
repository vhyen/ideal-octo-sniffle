from pyspark.sql import SparkSession
from pyspark import SparkConf
import configparser
import os
from datetime import datetime


S3_BUCKET_NAME = "fsa-de"
S3_FILE_KEY = "shuffled_numbers.txt"

def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf

def is_prime(n):
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    
    for i in range(3, int(n ** 0.5) + 1, 2):
        if n % i == 0:
            return False
    return True

if __name__ == '__main__':
    conf = get_spark_app_config()
    # logging.basicConfig(level=print)
    # assignment/ideal-octo-sniffle/data/data_1mil.txt
    start_time = datetime.now()
    spark = SparkSession.builder\
        .config("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("com.amazonaws.services.s3.enableV4", "true") \
        .config("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config(conf=conf) \
        .getOrCreate()
    print(f"Spark session started in: {(datetime.now() - start_time).total_seconds()} seconds")
    
    start_time = datetime.now()
    s3_path = f"s3a://{S3_BUCKET_NAME}/{S3_FILE_KEY}"
    rdd = spark.sparkContext.textFile(s3_path) 
    rdd = rdd.map(lambda x: int(x) if x.isdigit() else None).filter(lambda x: x is not None) 
    print(f"RDD created in: {(datetime.now() - start_time).total_seconds()} seconds")

    start_time = datetime.now()
    rdd.repartition(5)
    prime_rdd = rdd.filter(is_prime)
    prime_numbers = prime_rdd.collect()
    print(f"Prime numbers filtered in: {(datetime.now() - start_time).total_seconds()} seconds")
    

    timestamp = datetime.now().strftime('%H:%M_%d-%m')

    output_dir = os.path.join(os.getcwd(), 'prime_numbers')
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{rdd.count()}_num_{timestamp}.txt")

    try:
        start_time = datetime.now()
        with open(output_path, 'w') as f:
            f.write('\n'.join(map(str, prime_numbers)))
        print(f"Prime numbers saved to: {output_path}")
        print(f"Prime numbers saved in: {(datetime.now() - start_time).total_seconds()} seconds")
    except Exception as e:
        print(f"Error saving file: {str(e)}")
    finally:
        spark.stop()

        