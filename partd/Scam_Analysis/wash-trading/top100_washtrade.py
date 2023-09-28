import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
import pyspark.sql.functions as F
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
    
    def check_transactions(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            int(fields[3])
            float(fields[7])
            return True
        except:
            return False
 
    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    trans = transactions.filter(check_transactions)
    
    trans_map = trans.map(
        lambda x: (
            x.split(',')[3],
            x.split(',')[5],
            x.split(',')[6], 
            x.split(',')[7], 
            x.split(',')[11]      
        )
    )

    # Define column names for transaction map
    naming_columns = ['block_number', 'from_address', 'to_address', 'value', 'timestamp']

    # Convert transaction map to a Spark DataFrame
    df = trans_map.toDF(naming_columns)

    # Define a window function to calculate lagged values
    w = Window.partitionBy('block_number', 'from_address', 'to_address', 'value').orderBy('timestamp')

    # Group DataFrame by block number, to address, and value; calculate total value, count, max timestamp, and count by partition
    df = df.withColumn('block_count', F.count('*').over(w))

    # Filter for wash trades
    wash_trades = df.filter((F.col('block_count') > 1) | (F.col('from_address') == F.col('to_address')))

    # Map and aggregate
    trans_rdd = wash_trades.rdd.map(lambda x: ((x[0], x[1]), float(x[3]))) 
    output = trans_rdd.reduceByKey(lambda x, y: x + y)
    top100 = output.takeOrdered(100, key=lambda x: -x[1])

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_partd2_' + date_time + '/top100_washtrade.txt')
    my_result_object.put(Body=json.dumps(top100))

    
    spark.stop()