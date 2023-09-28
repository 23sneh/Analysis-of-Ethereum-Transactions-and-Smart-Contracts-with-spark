import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Ethereum")\
        .getOrCreate()

    
    # shared read-only object bucket containing datasets
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
    
    block_schema = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv")
    # clean_lines=blocks.filter(check_blocks)
    # bf = clean_lines.map(features_blocks)
    # blocks_reducing = bf.reduceByKey(lambda a, b: a + b)
    # top10_miners = blocks_reducing.takeOrdered(10, key=lambda l: -l[1])

     # filter valid blocks
    filtered_blocks = block_schema.filter(lambda line: len(line.split(',')) == 19 and line.split(',')[1] != 'hash')
    
    # extract miner and block size information
    mapping_miner_and_block_size = filtered_blocks.map(lambda line: (line.split(',')[9], int(line.split(',')[12])))
    
    # reduce by key to calculate total block size mined by each miner
    size_of_miner_block = mapping_miner_and_block_size.reduceByKey(operator.add)
    
    # sort by total block size and take top 10 miners
    top10_miners = size_of_miner_block.takeOrdered(10, key=lambda x: -x[1])
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_partc_' + date_time + '/top10_miners.txt')
    
    my_result_object.put(Body=json.dumps(top10_miners))
    
    spark.stop()