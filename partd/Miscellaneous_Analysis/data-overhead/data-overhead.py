import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col, length


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
    
    
    # Load the blocks dataset into a PySpark DataFrame
    blocks_df = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv", header=True, inferSchema=True)


    # Calculate the size of each hex column in bytes
    hex_cols = ["sha3_uncles", "logs_bloom", "transactions_root", "state_root", "receipts_root"]
    total_size_hex_bytes = sum(blocks_df.select([length(col(c)) * 1 / 4 for c in hex_cols]).first())
    total_size_hex_mb = total_size_hex_bytes / 1024 / 1024

    # Calculate the total size of the remaining columns
    remaining_cols = [c for c in blocks_df.columns if c not in hex_cols]
    remaining_size_mb = blocks_df.select([length(col(c)) for c in remaining_cols]).rdd.map(lambda row: row[0]).sum() / 1024 / 1024

    # Calculate the percentage of space that would be saved by removing the hex columns
    space_saved_percent = (total_size_hex_mb / (total_size_hex_mb + remaining_size_mb)) * 100

    print("Total size of hex columns to be removed: {:.2f} MB".format(total_size_hex_mb))
    print("Total size of remaining columns: {:.2f} MB".format(remaining_size_mb))
    print("Percentage of space saved by removing hex columns: {:.2f}%".format(space_saved_percent))

    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_partd5_' + date_time + '/total_hex_size.txt')
    my_result_object.put(Body=json.dumps(total_size_hex_mb))
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_partd5_' + date_time + '/remaining_size.txt')
    my_result_object.put(Body=json.dumps(remaining_size_mb))               
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_partd5_' + date_time + '/space_saved.txt')
    my_result_object.put(Body=json.dumps(space_saved_percent))               

    
    spark.stop()