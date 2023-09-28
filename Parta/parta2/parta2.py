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

    def good_line(line):
        fields = line.split(',')
        if len(fields) != 15:
            return False
        try:
            float(fields[7])
            int(fields[11])
            return True
        except:
            return False

    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']
    
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoop_conf.set("fs.s3a.access.key", s3_access_key_id)
    hadoop_conf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    
    sneh = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    clean_lines = sneh.filter(good_line)
    
    def map_transactions(line):
        fields = line.split(',')
        block_timestamp = int(fields[11])
        value = float(fields[7])
        date = time.strftime("%m/%Y", time.gmtime(int(block_timestamp)))
        return (date, (value, 1))

    date_vals = clean_lines.map(map_transactions)
    reduce = date_vals.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    avg_transactions = reduce.map(lambda a: (a[0], str(a[1][0] / a[1][1])))
    out = avg_transactions.map(lambda s: ','.join(str(alll) for alll in s))
                                            
    now = datetime.now()
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
        
    s3_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    result_object = s3_resource.Object(s3_bucket, 'ethereum_avg_' + date_time + '/parta2.txt')
    result_object.put(Body='\n'.join(out.take(100)))
#   result_object.put(Body='\n'.join(out))
    
    spark.stop()
