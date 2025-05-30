import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, array, col, struct, explode, concat_ws, lit
from pyspark.sql.types import StringType
import time
import boto3
from urllib.parse import urlparse

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

# Configuration
execute_platform = 'aws'  # Always 'aws' for Glue
file_path_local = r"C:\Users\panisarak\Desktop\BigDataHW\aws\data\raw-data\social_media_posts\2025-05\product_reviews_final.json"
result_path_local = r"C:\Users\panisarak\Desktop\BigDataHW\aws\data\processed-data\etl-outputs\social_media_posts\before_cleansing\2025-05\customer_mention_texts.parquet"
file_path_aws = 's3://bigdata-project-group/data/raw-data/social_media_posts/2025-05/'
result_path_aws = 's3://bigdata-project-group/data/processed-data/etl-outputs/social_media_posts/before_cleansing/2025-05/'


def select_path(execute_platform):
    """Select appropriate file paths based on execution platform"""
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


def transpose_dataframe(file_path) -> DataFrame:
    """
    Read JSON files and transpose the nested TextItem structure using pure Spark operations
    """
    print(f"Processing JSON files from: {file_path}")
    
    try:
        # Read all JSON files in the S3 folder or local path
        if file_path.startswith("s3://"):
            # For S3, read all JSON files in the folder
            df = spark.read.option("multiline", "true").json(file_path + "*.json")
        else:
            # For local file, read the specific file
            df = spark.read.option("multiline", "true").json(file_path)
        
        print("Initial schema:")
        df.printSchema()
        print(f"Initial row count: {df.count()}")
        
        # Check if TextItem exists in the schema
        if "TextItem" not in df.columns:
            print("Warning: TextItem column not found in JSON. Available columns:", df.columns)
            return df
        
        # Handle the nested structure under TextItem
        df_subscr = df.select(F.expr("TextItem.*"))
        
        print("After selecting TextItem contents:")
        df_subscr.printSchema()
        df_subscr.show(2, truncate=False)
        
        # Get all column names from the TextItem structure
        column_names = df_subscr.columns
        print(f"Found columns in TextItem: {column_names}")
        
        # Create struct array for transposition
        struct_array = F.array(*[
            F.struct(F.lit(col_name).alias("id"), F.col(col_name).alias("text"))
            for col_name in column_names
        ])
        
        # Explode the struct array to transpose the data
        df_transposed = df_subscr.select(
            F.explode(struct_array).alias("col")
        ).select(
            F.col("col.id").alias("id"),
            F.col("col.text").alias("text")
        )
        
        # Handle different text data types
        # If text is an array, concatenate with spaces
        # If text is already a string, keep as is
        df_transposed = df_transposed.withColumn(
            "text",
            F.when(F.col("text").isNull(), F.lit(""))
             .when(F.size(F.col("text")) > 0, F.concat_ws(" ", F.col("text")))
             .otherwise(F.col("text").cast("string"))
        )
        
        # Filter out null or empty text entries
        df_transposed = df_transposed.filter(
            (F.col("text").isNotNull()) & 
            (F.trim(F.col("text")) != "")
        )
        
        print("Final transposed schema:")
        df_transposed.printSchema()
        print(f"Final row count: {df_transposed.count()}")
        
        return df_transposed
        
    except Exception as e:
        print(f"Error in transpose_dataframe: {e}")
        raise


def check_files_in_s3_path(bucket_name, prefix):
    """Check if files exist in S3 path"""
    try:
        s3 = boto3.resource('s3')
        all_objects = s3.Bucket(bucket_name).objects.filter(Prefix=prefix)
        for _ in all_objects:
            print("---------------------- There is a file existing in the path ----------------------")
            return True
        print("---------------------- No files found in the path ----------------------")
        return False
    except Exception as e:
        print(f"Error checking S3 path: {e}")
        return False


def write_parquet_safely_spark(df, execute_platform, result_path, max_retries=5):
    """
    Write DataFrame to Parquet using pure Spark operations
    """
    if execute_platform == 'aws':
        try:
            parsed = urlparse(result_path)
            bucket_name = parsed.netloc
            prefix = parsed.path.lstrip('/')

            print("---------------------- Writing data to S3 using Spark... ----------------------")
            print(f"Target S3 path: {result_path}")
            
            # Write using Spark's native parquet writer
            df.coalesce(1) \
              .write \
              .mode("overwrite") \
              .option("compression", "gzip") \
              .parquet(result_path)
            
            print(f"---------------------- Saved file to S3: {result_path} ----------------------")

            # Verify file existence with retries
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
            
            return True

        except Exception as e:
            print(f"Failed to write to S3. Error: {e}")
            return False

    else:  # Local execution
        try:
            print(f"---------------------- Writing data locally: {result_path} ----------------------")
            
            df.coalesce(1) \
              .write \
              .mode("overwrite") \
              .option("compression", "gzip") \
              .parquet(result_path)
            
            print(f"Successfully wrote file locally: {result_path}")
            return True

        except Exception as e:
            print(f"Failed to write locally. Error: {e}")
            return False


def main():
    """Main processing function"""
    try:
        print("Starting JSON to Parquet conversion...")
        
        # Get file paths
        input_path, result_path = select_path(execute_platform)
        print(f'Input path: {input_path}')
        print(f'Result path: {result_path}')
        
        # Process the JSON data
        df_transposed = transpose_dataframe(input_path)
        
        # Show sample data
        print("Sample of transposed data:")
        df_transposed.show(5, truncate=False)
        
        # Write the result
        success = write_parquet_safely_spark(df_transposed, execute_platform, result_path)
        
        if success:
            print("JSON to Parquet conversion completed successfully!")
        else:
            print("JSON to Parquet conversion completed but file write may have failed")
            
        # Show final statistics
        final_count = df_transposed.count()
        print(f"Total records processed: {final_count}")
            
    except Exception as e:
        print(f"Error in main processing: {e}")
        raise


if __name__ == '__main__':
    sns = boto3.client('sns')
    SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:851725315772:alert-workflow-failed"
    try :
        main()
        job.commit()
    except Exception as error :
        alert_msg = f"ALERT: Workflow failed.Error is {error}"
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=alert_msg
        )
    