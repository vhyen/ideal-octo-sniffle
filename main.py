# import sys
from pyspark.sql import SparkSession
from pyspark import SparkConf
import configparser
import os
import shutil
from datetime import datetime

NUM_NUMBERS = 1000000

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
    # Only check odd numbers up to square root
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
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    # rdd = spark.read.csv('arn:aws:s3:ap-southeast-1:381491951595:accesspoint/fsa-de-team-01')
    # print(rdd.collect())

    # # print(rdd.collect())

    rdd = spark.sparkContext.parallelize(range(1, NUM_NUMBERS + 1, 1))

    # numeric_rdd = rdd.map(lambda x: int(x) if x.isdigit() else None).filter(lambda x: x is not None)

    # prime_rdd = numeric_rdd.filter(is_prime)
    # rdd.repartition(3)

    prime_rdd = rdd.filter(is_prime)

    prime_numbers = prime_rdd.collect()
    
    # Save to local file on driver
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

        