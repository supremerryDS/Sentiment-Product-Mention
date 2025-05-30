#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import json
import re
import os
from urllib.parse import urlparse
import time
import boto3

execute_platform = 'aws'  # 'local' or 'aws'
file_path_local = r"C:\Users\panisarak\Desktop\BigDataHW\aws\data\raw-data\products_catelog\2025-04\products.json"
file_path_aws = 's3://bigdata-project-group/data/raw-data/products_catelog/2025-04/products.json'
result_path_local = r'C:\Users\panisarak\Desktop\BigDataHW\aws\data\processed-data\etl-outputs\product_catelog\2025-04\product_catelog.parquet.gzip'
result_path_aws = 's3://bigdata-project-group/data/processed-data/etl-outputs/product_catelog/before_cleansing/2025-04/'

def select_path(execute_platform):
    if execute_platform == 'local':
        return file_path_local, result_path_local
    elif execute_platform == 'aws':
        return file_path_aws, result_path_aws
    else:
        raise ValueError("Invalid environment. Choose 'local' or 'aws'.")

def transform_to_df(file_path):
    df_list = []
    if execute_platform == 'local':
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            product_list = list(data['Product'].keys())
            product_detail = list(data['Product'].values())
            df = pd.DataFrame(product_detail, columns=['product_name', 'product_category', 'price'])
            df['product_id'] = product_list
            df = df[['product_id', 'product_name', 'product_category', 'price']]
    else:
        import boto3
        s3 = boto3.client('s3')
        parsed = urlparse(file_path)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip('/')
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        json_keys = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.json')]
        for key in json_keys:
            obj = s3.get_object(Bucket=bucket, Key=key)
            content = obj['Body'].read().decode('utf-8')
            data = json.loads(content)
            product_list = list(data['Product'].keys())
            product_detail = list(data['Product'].values())
            df = pd.DataFrame(product_detail, columns=['product_name', 'product_category', 'price'])
            df['product_id'] = product_list
            df = df[['product_id', 'product_name', 'product_category', 'price']]

    for col_name in df.select_dtypes(include='object').columns:
        df[col_name] = (
            df[col_name]
            .map(lambda x: x.strip() if isinstance(x, str) else x)
            .map(lambda x: re.sub(r'\s+', ' ', x) if isinstance(x, str) else x)
            .map(lambda x: re.sub(r'\s*-\s*', '-', x) if isinstance(x, str) else x)
            .map(lambda x: re.sub(r'\s*/\s*', '/', x) if isinstance(x, str) else x)
        )
    df = df.drop_duplicates(subset='product_id', keep='first')
    return df

def make_path_string(result_path):
    if result_path.startswith("s3://"):
        return result_path
    else:
        result_path = os.path.abspath(result_path)
        os.makedirs(os.path.dirname(result_path), exist_ok=True)
        return result_path

def write_parquet_safely(df, execute_platform, result_path, max_retries=5):
    if not isinstance(df, pd.DataFrame):
        df = df.toPandas()
    if execute_platform == 'aws':
        import boto3
        spark = SparkSession.builder.getOrCreate()
        parsed = urlparse(result_path)
        bucket_name = parsed.netloc
        prefix = parsed.path.lstrip('/')
        spark_df = spark.createDataFrame(df)
        spark_df.write.mode("overwrite").option("compression", "gzip").parquet(result_path)
        s3 = boto3.resource('s3')
        all_objects = list(s3.Bucket(bucket_name).objects.filter(Prefix=prefix))
        return len(all_objects) > 0
    else:
        try:
            table = pa.Table.from_pandas(df)
            pq.write_table(table, result_path, compression='gzip')
            print(f"Successfully wrote file locally: {result_path}")
            return True
        except Exception as e:
            print(f"Failed to write locally. Error: {e}")
            return False

def main():
    input_path, result_path = select_path(execute_platform)
    print('Input Path:', input_path)
    print('Result Path:', result_path)
    df = transform_to_df(input_path)
    make_path_string(result_path)
    write_parquet_safely(df, execute_platform, result_path)

if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(f"ERROR: Workflow failed. {error}")
