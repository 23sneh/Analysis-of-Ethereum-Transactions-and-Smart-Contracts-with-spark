import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, col, month, avg

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

    # Filter out invalid transactions
    def check_transactions(line):
        try:
            fields = line.split(',')
            if len(fields) != 15:
                return False
            float(fields[9])
            float(fields[11])
            return True
        except:
            return False

    transactions_rdd = transactions.filter(check_transactions)

    # Map each transaction to a tuple of (month, (gas_price, 1))
    def map_transactions(line):
        fields = line.split(',')
        block_timestamp = int(fields[11])
        gas_price = float(fields[9])
        month = time.strftime('%m/%Y', time.gmtime(block_timestamp))
        return (month, (gas_price, 1))

    gas_prices_rdd = transactions_rdd.map(map_transactions)

    # Reduce by key to get the sum of gas prices and count of transactions for each month
    gas_prices_reduced_rdd = gas_prices_rdd.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))

    # Map each month to a tuple of (month, average_gas_price)
    average_gas_prices_rdd = gas_prices_reduced_rdd.map(lambda x: (x[0], x[1][0]/x[1][1]))

    # Sort the RDD by month
    sorted_average_gas_prices_rdd = average_gas_prices_rdd.sortByKey(ascending=True)

    # Collect the results as a list of tuples
    result = sorted_average_gas_prices_rdd.collect()
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_partd4_' + date_time + '/avg_gas_price.txt')
    my_result_object.put(Body=json.dumps(result))               

    
    spark.stop()