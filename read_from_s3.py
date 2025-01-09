from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
import configparser

S3_BUCKET_NAME = "fsa-de"
S3_FILE_KEY = "test.txt"

def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf

config = get_spark_app_config()

spark = SparkSession.builder.appName("PySpark") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2") \
    .config("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("com.amazonaws.services.s3.enableV4", "true") \
    .config("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config(conf=config) \
    .getOrCreate()

s3_path = f"s3a://{S3_BUCKET_NAME}/{S3_FILE_KEY}"
rdd = spark.sparkContext.textFile(s3_path)

print(rdd.collect())
