import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
import re
from pyspark.sql.functions import to_timestamp, from_unixtime, unix_timestamp, expr
from pyspark.sql.functions import *
import pyspark.sql.functions as F

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

    # read transactions and contracts datasets into dataframes
    transactions_df = spark.read.format("csv").option("header", "true").load("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    contracts_df = spark.read.format("csv").option("header", "true").load("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")

    # select relevant fields from transactions_df and contracts_df
    selected_transactions_df = transactions_df.select("to_address", "gas", "block_timestamp")
    selected_contracts_df = contracts_df.select("address")

    # define function to map transactions_df into required format
    def map_transactions(line):
        to_address = line.to_address
        gas = float(line.gas)
        block_timestamp = int(line.block_timestamp)
        date = time.strftime("%m/%Y", time.gmtime(block_timestamp))
        return (to_address, (date, gas))

    # map transactions_df using map_transactions() function
    mapped_transactions = selected_transactions_df.rdd.map(map_transactions)

    # define function to map contracts_df into required format
    def map_contracts(line):
        return (line.address, 1)

    # map contracts_df using map_contracts() function
    mapped_contracts = selected_contracts_df.rdd.map(map_contracts)

    # join mapped_transactions and mapped_contracts on to_address and address fields respectively
    joins = mapped_transactions.join(mapped_contracts)

    # define function to map joins into required format
    def reduce_joins(line):
        date = line[1][0][0]
        gas = line[1][0][1]
        return (date, (gas, 1))

    # reduce joins using reduce_joins() function
    reduced_joins = joins.map(reduce_joins).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))

    # define function to map reduced_joins into required format
    def calculate_average_gas_used(line):
        date = line[0]
        average_gas_used = str(line[1][0] / line[1][1])
        return (date, average_gas_used)

    # map reduced_joins using calculate_average_gas_used() function
    average_gas_used = reduced_joins.map(calculate_average_gas_used)
    
    sorted_average_gas_used = average_gas_used.collect()
    sorted_average_gas_used.sort(key=lambda x: x[1], reverse=True)

    # sort average_gas_used by date in ascending order
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    my_result_another_object = my_bucket_resource.Object(s3_bucket,'ethereum_partd400_' + date_time + '/avg_gas_used_year.txt')
    my_result_another_object.put(Body=json.dumps(sorted_average_gas_used))               

    
    spark.stop()