# Import libraries
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError 
import os
from datetime import datetime as dt
import yaml 
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

# Read in YAML for AWS creds
# Specify the path to your YAML file
yaml_fp = '/Users/samivanecky/git/draftkings/secrets.yaml'

# Read the YAML file
with open(yaml_fp, 'r') as file:
    creds = yaml.safe_load(file)

# Connect to S3 
s3 = boto3.resource(
    service_name='s3',
    region_name=creds['region_name'],
    aws_access_key_id=creds['aws_access_key_id'],
    aws_secret_access_key=creds['aws_secret_access_key']
)

# Create Spark session
spark = SparkSession \
    .builder \
    .appName("s3 connect") \
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.2') \
    .config('fs.s3a.aws.credentials.provider','org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.access.key", creds['aws_access_key_id']) \
    .config("spark.hadoop.fs.s3a.secret.key", creds['aws_secret_access_key']) \
    .getOrCreate()

# df = spark \
#     .read \
#     .format("csv") \
#     .option("header", True) \
#     .load("s3a://sgi-dk/data_2023-11-12_07-59-26.csv")

# List objects in S3
objs = list(s3.Bucket('sgi-dk').objects.all())

# Define list for objects to be deleted once they've been read into a dataframe
objs_to_del = []

# Iterate over objects and read into dataframe
for o in objs:
    print(f"Getting data for : {o.key}")
    # Read file into spark
    tmp_df = spark \
        .read \
        .format("csv") \
        .option("header", True) \
        .load(f"s3a://sgi-dk/{o.key}") \
        .drop('_c0')
    # Check if overall data exists and union
    try:
        df = df.union(tmp_df)
    except:
        df = tmp_df
    # Append name to objects to delete
    # objs_to_del = objs_to_del.append(str(o.key))

# Clean up data
df = df \
    .withColumn('spread_line', f.expr("substring(spread, 1, length(spread)-4)")) \
    .withColumn('spread_odds', f.expr("substring(spread, length(spread)-3, length(spread))")) \
    .withColumn('total_line', f.expr("substring(total, 1, length(total)-4)")) \
    .withColumn('total_odds', f.expr("substring(total, length(total)-3, length(total))")) \
    .withColumn('line_time_status', f.split(df.team, "PM|AM|Quarter")) \ 
    .withColumn('team_name', f.split(df.team, "PM|AM|Quarter", 2).getItem(1))