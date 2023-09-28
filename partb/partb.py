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
    
    
    transactions_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    contracts_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")

    
    good_transactions = transactions_lines.filter(lambda x: len(x.split(',')) == 15 and x.split(',')[3].isdigit())
    good_contracts = contracts_lines.filter(lambda x: len(x.split(',')) == 6)

    
    transaction_values = good_transactions.map(lambda x: (x.split(',')[6], int(x.split(',')[7])))
    total_values_in_address = transaction_values.reduceByKey(lambda x, y: x + y)
    contract_values = good_contracts.map(lambda x: (x.split(',')[0], x.split(',')[3]))
    total_values_in_contract = contract_values.reduceByKey(lambda x, y: int(x) + int(y))
    total_values_per_contract = total_values_in_contract.join(total_values_in_address).map(lambda x: (x[0], x[1][1]))
    top10_popular_services = total_values_per_contract.takeOrdered(10, key=lambda x: -x[1])
    
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_partb_' + date_time + '/top10_popular_services.txt')
    my_result_object.put(Body=json.dumps(top10_popular_services))

    spark.stop()