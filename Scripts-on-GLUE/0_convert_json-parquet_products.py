#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql.functions import array, col, struct, concat_ws
import json
import pandas as pd
import boto3
import re
import os
from urllib.parse import urlparse

execute_platform = 'aws' # 'local' or 'aws'
file_path_local = r"C:\Users\panisarak\Desktop\BigDataHW\aws\data\raw-data\products_catelog\2024-04\products.json"
file_path_aws = 's3://bigdata-project-group/data/raw-data/products_catelog/2024-04/products.json'
result_path_local = r'C:\Users\panisarak\Desktop\BigDataHW\aws\data\processed-data\etl-outputs\product_catelog\2025-04\product_catelog.parquet.gzip' # "C:\Users\panisarak\Desktop\BigDataHW\result_save_to_parquet"
result_path_aws = 's3://bigdata-project-group/data/processed-data/etl-outputs/product_catelog/before_cleansing/2025-04/'

def select_path(execute_platform):
    try:
        if execute_platform == 'local':
            return file_path_local, result_path_local
        elif execute_platform == 'aws':
            return file_path_aws, result_path_aws
        else:
            raise ValueError(">> Invalid environment specified. Choose 'local' or 'aws'.")
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

def transform_to_df(file_path):
    parsed = urlparse(file_path)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip('/')

    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if 'Contents' not in response:
        print(f"No files found at {file_path}")
        return pd.DataFrame()

    json_keys = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.json')]
    
    if not json_keys:
        print(f"No JSON files found in {file_path}")
        return pd.DataFrame()

    df_list = []

    for key in json_keys:
        print(f"Reading: s3://{bucket}/{key}")
        obj = s3.get_object(Bucket=bucket, Key=key)
        content = obj['Body'].read().decode('utf-8')
        data = json.loads(content)

        product_list = list(data['Product'].keys())
        product_detail = list(data['Product'].values())

        df = pd.DataFrame(product_detail, columns=['product_name', 'product_category', 'price'])
        df['product_id'] = product_list
        df = df[['product_id', 'product_name', 'product_category', 'price']]

    # ---------- to clean data ---------- #
        # - multiple whitespaces -> single whitespace
        # - strip
        # - subtract whitespace before and after '-'
        # - subtract whitespace before and after '/'
     # ----------- ---------- ----------- #

    # ---------- Clean text columns ---------- #
        for col_name in df.select_dtypes(include='object').columns:
            df[col_name] = (
                df[col_name]
                .map(lambda x: x.strip() if isinstance(x, str) else x)
                .map(lambda x: re.sub(r'\s+', ' ', x) if isinstance(x, str) else x)
                .map(lambda x: re.sub(r'\s*-\s*', '-', x) if isinstance(x, str) else x)
                .map(lambda x: re.sub(r'\s*/\s*', '/', x) if isinstance(x, str) else x)
            )
        
        df_list.append(df)

    merged_df = pd.concat(df_list, ignore_index=True)
    merged_df = merged_df.drop_duplicates(subset='product_id', keep='first')
    return merged_df

def make_path_string(result_path):
    if result_path.startswith("s3://"):
        # Do nothing for S3 paths
        print(f"S3 path detected: {result_path} — skipping directory creation.")
        return result_path
    else:
        result_path = os.path.abspath(result_path)
        os.makedirs(os.path.dirname(result_path), exist_ok=True)
        return result_path

def check_files_in_s3_path(bucket_name, prefix):
    s3 = boto3.resource('s3')
    all_objects = s3.Bucket(bucket_name).objects.filter(Prefix=prefix)
    for _ in all_objects:
        print("---------------------- There is a file existing in the path ----------------------")
        return True
    print("---------------------- No files found in the path ----------------------")
    return False

def write_parquet_safely(df, execute_platform, result_path, max_retries=5):
    if not isinstance(df, pd.DataFrame):
        df = df.toPandas()

    if execute_platform == 'aws':
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()

            parsed = urlparse(result_path)
            bucket_name = parsed.netloc
            prefix = parsed.path.lstrip('/')

            print("---------------------- Writing data to S3... ----------------------")
            spark_df = spark.createDataFrame(df)
            spark_df.write.mode("overwrite").option("compression", "gzip").parquet(result_path)
            print(f"---------------------- Saved file to S3: {result_path} ----------------------")

            retry_count = 0
            file_exists = False

            while not file_exists and retry_count < max_retries:
                file_exists = check_files_in_s3_path(bucket_name, prefix)
                if file_exists:
                    print("---------------------- File confirmed to exist in the path ----------------------")
                    return True
                else:
                    retry_count += 1
                    print(f"Retry {retry_count}/{max_retries} - Waiting before next check...")
                    time.sleep(15)

            if not file_exists:
                print("---------------------- Max retries reached. File not confirmed. ----------------------")
                return False
        except Exception as e:
            print(f"Failed to write to S3. Error: {e}")
            return False

    else:  # Local
        try:
            dir_path = os.path.dirname(result_path)
            if os.path.exists(dir_path):
                if os.path.isfile(dir_path):
                    os.remove(dir_path)
            else:
                os.makedirs(dir_path, exist_ok=True)

            table = pa.Table.from_pandas(df)
            pq.write_table(table, result_path, compression='gzip')
            print(f"Successfully wrote file locally: {result_path}")
            return True

        except Exception as e:
            print(f"Failed to write locally. Error: {e}")
            return False
        
def main():
    input_path, result_path = select_path(execute_platform)
    print('input_path: ', input_path)
    print('result_path: ', result_path)

    df = transform_to_df(input_path)
    print(df)
    make_path_string(result_path)
    write_parquet_safely(df, execute_platform, result_path)

if __name__ == '__main__':
    sns = boto3.client('sns')
    SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:416191274488:alert-workflow-failed"
    try :
        main()
    except Exception as error :
        alert_msg = f"ALERT: Workflow failed.Error is {error}"
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=alert_msg
        )