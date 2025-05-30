import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import os
from urllib.parse import urlparse
import time  # needed for sleep in S3 write retry

## csv -> convert to parquet
execute_platform = 'aws' # 'local' or 'aws'
amazon_path_local = r"C:\Users\panisarak\Desktop\BigDataHW\aws\data\raw-data\ecommerce_reviews\amazon\2025-05\amazon_reviews.csv"
ebay_path_local = r"C:\Users\panisarak\Desktop\BigDataHW\aws\data\raw-data\ecommerce_reviews\ebay\2025-05\ebay_reviews.csv"
result_path_local = r"C:\Users\panisarak\Desktop\BigDataHW\aws\data\processed-data\etl-outputs\e-commerce_reviews\before_cleansing\merge_amazon-ebay\2025-05\merge_ecommerce_reviews.parquet.gzip"
amazon_path_aws = 's3://bigdata-project-group/data/raw-data/ecommerce_reviews/amazon/2025-05/'
ebay_path_aws = 's3://bigdata-project-group/data/raw-data/ecommerce_reviews/ebay/2025-05/'
result_path_aws = 's3://bigdata-project-group/data/processed-data/etl-outputs/e-commerce_reviews/before_cleansing/merge_amazon-ebay/2025-05/'

def select_path(execute_platform):
    try:
        if execute_platform == 'local':
            return amazon_path_local, ebay_path_local, result_path_local
        elif execute_platform == 'aws':
            return amazon_path_aws, ebay_path_aws, result_path_aws
        else:
            raise ValueError(">> Invalid environment specified. Choose 'local' or 'aws'.")
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

def read_all_csvs_from_s3(s3_path):
    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip('/')

    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if 'Contents' not in response:
        print(f"No files found at {s3_path}")
        return pd.DataFrame()

    csv_keys = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.csv')]

    if not csv_keys:
        print(f"No CSV files found in {s3_path}")
        return pd.DataFrame()

    print(f"Found {len(csv_keys)} CSV files in {s3_path}")
    
    # Read and concatenate all CSVs
    df_list = []
    for key in csv_keys:
        file_path = f's3://{bucket}/{key}'
        print(f"Reading: {file_path}")
        df = pd.read_csv(file_path)
        df_list.append(df)

    return pd.concat(df_list, ignore_index=True)


def merge_dataframe(df1, df2):
    # Define the target column names
    target_columns = ['product_id', 'product_name', 'rating', 'review_text', 'sentiment', 'platform']

    # Rename columns of df1 and df2 (ensure both have the same structure)
    df1.columns = target_columns
    df2.columns = target_columns

    # Merge the two DataFrames
    df_merged = pd.concat([df1, df2], ignore_index=True)

    # Return only the selected columns
    return df_merged

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
    amazon_path, ebay_path, result_path = select_path(execute_platform)

    if execute_platform == 'aws':
        amazon = read_all_csvs_from_s3(amazon_path)
        amazon['platform'] = 'amazon'

        ebay = read_all_csvs_from_s3(ebay_path)
        ebay['platform'] = 'ebay'
    elif execute_platform == 'local':
        amazon = pd.read_csv(amazon_path)
        amazon['platform'] = 'amazon'

        ebay = pd.read_csv(ebay_path)
        ebay['platform'] = 'ebay'

    df_merged = merge_dataframe(amazon, ebay)
    make_path_string(result_path)
    write_parquet_safely(df_merged, execute_platform, result_path)

if __name__ == '__main__':
    sns = boto3.client('sns')
    SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:851725315772:alert-workflow-failed"
    try :
        main()
    except Exception as error :
        alert_msg = f"ALERT: Workflow failed.Error is {error}"
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=alert_msg
        )
