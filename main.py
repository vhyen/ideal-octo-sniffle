# import sys
from pyspark.sql import SparkSession
from pyspark import SparkConf
import configparser
import os
import shutil
from datetime import datetime

# NUM_NUMBERS = 1000000
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

# Delete the folder if it exists
def delete_exist_folder(folder_path):
    if os.path.exists(folder_path):
        try:
            shutil.rmtree(folder_path)
            print(f"Folder '{folder_path}' deleted successfully.")
        except OSError as e:
            print(f"Error deleting folder '{folder_path}': {e}")
    else:
        print(f"Folder '{folder_path}' does not exist.")

if __name__ == '__main__':
    conf = get_spark_app_config()
    # spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    spark = SparkSession.builder\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2") \
        .config("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("com.amazonaws.services.s3.enableV4", "true") \
        .config("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config(conf=conf) \
        .getOrCreate()
    
    # rdd = spark.sparkContext.parallelize(range(1, NUM_NUMBERS + 1, 1))
    

    s3_path = f"s3a://{S3_BUCKET_NAME}/{S3_FILE_KEY}"
    rdd = spark.sparkContext.textFile(s3_path) 

    rdd = rdd.map(lambda x: int(x) if x.isdigit() else None).filter(lambda x: x is not None) 
    

    prime_rdd = rdd.filter(is_prime)

    prime_numbers = prime_rdd.collect()
    

    # timestamp = datetime.now().strftime('%Y:%m:%d:%H:%M')
    timestamp = datetime.now().strftime('%m:%d:%H:%M')

    output_path = os.path.join(os.getcwd(), 'prime_numbers', f"{rdd.count()}_num_{timestamp}.txt")
    try:
        with open(output_path, 'w') as f:
            f.write('\n'.join(map(str, prime_numbers)))
        print(f"Prime numbers saved to: {output_path}")
    except Exception as e:
        print(f"Error saving file: {str(e)}")
    finally:
        spark.stop()

        