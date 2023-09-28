import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import sum, count, abs, max, min, avg, lag, lead, col, when
from pyspark.sql.window import Window

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Ethereum")\
        .getOrCreate()

    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")  
    
    # Load the dataset into a DataFrame
    df = spark.read.format("csv").option("header", "true").load("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")

    wash_trade_df = df \
        .groupBy("from_address", "to_address", "value", "gas", "gas_price", "block_timestamp") \
        .agg(
            count("*").alias("tx_count"),
            avg("gas_price").alias("avg_gas_price"),
            lag("to_address").over(Window.partitionBy("from_address").orderBy("block_timestamp")).alias("prev_to_address")
        ) \
        .where(
            (col("tx_count") > 1) |
            (col("gas_price") > col("avg_gas_price")*1.5) |
            ((col("value") == 0) & (col("gas") > 21000) & (col("gas_price") > 1000000000)) |
            (col("prev_to_address").isNull() | (col("prev_to_address") != col("to_address")))
        ) \
        .select("from_address", "to_address", "value") \

    wash_trade_list = wash_trade_df.rdd.map(lambda x: (x.from_address, x.to_address, x.value)).take(50)

    print(wash_trade_list)    
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_partd2_' + date_time + '/50top_washtrade.txt')
    my_result_object.put(Body=json.dumps(wash_trade_list))
    
    spark.stop()