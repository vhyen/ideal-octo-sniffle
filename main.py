import sys
from pyspark.sql import SparkSession

from utils.logger import Log4j
from utils.utils import *


if __name__ == '__main__':
    conf = get_spark_app_config()
    spark = SparkSession.builder.config(conf=conf).appName("PrimeNumbers").master("local[3]").getOrCreate()
    
    rdd = spark.sparkContext.textFile("data/nums.txt")
    # print(rdd.collect())

    numeric_rdd = rdd.map(lambda x: int(x) if x.isdigit() else None).filter(lambda x: x is not None)

    prime_rdd = numeric_rdd.filter(is_prime)
    # print(prime_rdd.collect())
    
    delete_exist_folder("prime_numbers")
    prime_rdd.repartition(1).saveAsTextFile("prime_numbers")