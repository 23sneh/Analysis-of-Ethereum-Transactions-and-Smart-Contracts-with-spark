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
    
    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    scams = spark.read.json("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/scams.json")

    CATEGORY_MAP = {
        'Phishing': 'Scam',
        'Fake ICO': 'Scam',
        'Scamming': 'Scam',
    }

    def is_valid_transaction(line):
        fields = line.split(',')
        return len(fields) == 15 and fields[11].isdigit() and isinstance(fields[6], str) and isinstance(float(fields[7]), float)

    def is_valid_scam(scam):
        if scam is None or scam.get('id') is None or scam.get('category') is None or scam.get('index') is None:
            return False
        for address_data in scam['result']:
            address_info = scam['result'][address_data]

            identity = str(address_info['id'])
            name = str(address_info['name'])
            url = str(address_info['url'])
            coin = str(address_info['coin'])
            category = CATEGORY_MAP.get(address_info['category'], address_info['category'])
            subcategory = str(address_info.get('subcategory', ''))

            for i, address in enumerate(address_info['addresses']):
                index = str(i)

        return True

    def extract_transaction_fields(line):
        fields = line.split(',')
        return ((fields[5], fields[6]), float(fields[7]))

    def extract_scam_fields(scam):
        data = scam.asDict()
        addresses = [addr for addr in data['addresses']]
        return ((addresses[0], addresses[-1]), data['coin'])

    valid_transactions = transactions.filter(is_valid_transaction)
    valid_scams = scams.rdd.filter(lambda scam: is_valid_scam(scam.asDict()))

    transaction_amounts = valid_transactions.map(extract_transaction_fields)
    scam_details = valid_scams.map(extract_scam_fields)
    
    print('transactions:',transaction_amounts)
    print('scams:',scam_details)               
          
    spark.stop()