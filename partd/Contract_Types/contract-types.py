import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, sum, when
from pyspark.sql.types import IntegerType

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

    # Load transactions RDD and filter out rows with incorrect values
    transactions_rdd = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")

    # Load contracts RDD and filter out rows with incorrect values
    contracts_rdd = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")

    # Split transactions RDD into individual fields and create key-value pairs with to_address as key
    transactions_kv = transactions_rdd.map(lambda row: (row.split(",")[5], (1, float(row.split(",")[8]), float(row.split(",")[9]), float(row.split(",")[12]) + float(row.split(",")[13]), row.split(",")[14], float(row.split(",")[4]), row.split(",")[10], row.split(",")[3])))

    # Split contracts RDD into individual fields and create key-value pairs with address as key
    contracts_kv = contracts_rdd.map(lambda row: (row.split(",")[0], (float(row.split(",")[5]), row.split(",")[3] + row.split(",")[4] if row.split(",")[3] == "1" or row.split(",")[4] == "1" else "Other")))

    # Join the two RDDs and create a new key-value pair with contract address as key and all desired features as values
    joined_rdd = transactions_kv.join(contracts_kv).map(lambda x: (x[1][1][0], (1, 1 if x[1][0][0] is not None else 0, x[1][0][1], x[1][0][2], x[1][1][1], (x[1][0][5] - x[1][1][0]).astype(int), "Contract Creation" if x[1][0][0] is None else "Contract Interaction", x[1][0][3] + x[1][0][4])))

    # Reduce by key to obtain desired features using aggregation functions
    features_rdd = joined_rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3], x[4], x[5], x[6], x[7] + y[7]))

    # Filter out contracts with zero ether held
    filtered_rdd = features_rdd.filter(lambda x: x[1][2] > 0)
    
    # Create a list of tuples with contract address, ether held, and type
    contracts_list = filtered_rdd.map(lambda x: (x[0], x[1][2], x[1][4])).collect()

    all_contracts = [(address, ether_held, contract_type) for (address, ether_held, contract_type) in contracts_list]
    
    # save output
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
        
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
            
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_partd3_' + date_time + '/different-contracts.txt')
    my_result_object.put(Body=json.dumps(all_contracts.take(100)))
    
    spark.stop()
