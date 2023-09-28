import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col, sum, explode, lit, when

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
    
    def good_line_scams(line):
        try:
            fields = line.split(',')
            if len(fields)!=9:
                return False
            str(fields[4])
            str(fields[6])
            int(fields[0])
            return True
        except:
            return False      
        
    def good_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            int(fields[3])
            float(fields[7])
            int(fields[11])
            str(fields[6])
            return True
        except:
            return False

    transaction_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    scams_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/scams2.csv")
    
    transaction_clean_lines = transaction_lines.filter(good_line)
    scams_clean_lines = scams_lines.filter(good_line_scams)

    scams_filter = scams_clean_lines.map(lambda l: (l.split(',')[6], (l.split(',')[0], l.split(',')[4])))
    transaction_map = transaction_clean_lines.map(lambda l: (l.split(',')[6], float(l.split(',')[7])))
    joins = transaction_map.join(scams_filter)
    mapping = joins.map(lambda x: ((x[1][1][0], x[1][1][1]), x[1][0]))
    popular_scams = mapping.reduceByKey(lambda a, b: a + b).map(lambda a: ((a[0][0], a[0][1]), float(a[1])))
    popular_scams_10 = popular_scams.takeOrdered(10, key=lambda l: -1*l[1])
    print(popular_scams.take(10))
    
    scams_filter1 = scams_clean_lines.map(lambda x: (x.split(',')[6], x.split(',')[4]))
    transaction_map1 = transaction_clean_lines.map(lambda x: (x.split(',')[6], (time.strftime("%m/%Y", time.gmtime(int(x.split(',')[11]))), x.split(',')[7])))
    joins1 = transaction_map1.join(scams_filter1)
    mapping1 = joins1.map(lambda x: ((x[1][0][0], x[1][1]), x[1][0][1]))
    ethertimeseries = mapping1.reduceByKey(lambda a, b: a + b).map(lambda a: ((a[0][0], a[0][1]), float(a[1])))
    print(ethertimeseries.take(10))

    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_PartD_scam' + date_time + '/top_lucrative_scams.txt')
    my_result_object.put(Body=json.dumps(popular_scams_10))
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_PartD_scam' + date_time + '/ethertimeseries.txt')
    my_result_object.put(Body=json.dumps(ethertimeseries.collect()))


    spark.stop()