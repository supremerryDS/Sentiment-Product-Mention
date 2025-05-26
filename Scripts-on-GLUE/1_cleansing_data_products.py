import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, regexp_replace, lower, trim, when, isnan, isnull
from pyspark.sql.types import StringType
import time
import boto3
import re
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
file_path_local = r"C:\Users\panisarak\Desktop\BigDataHW\aws\data\processed-data\etl-outputs\product_catelog\before_cleansing\2025-04\product_catelog.parquet.gzip"
result_path_local = r"C:\Users\panisarak\Desktop\BigDataHW\aws\data\processed-data\etl-outputs\product_catelog\after_cleansing\2025-04\product_catelog_after_cleansing.parquet.gzip"
file_path_aws = 's3://bigdata-project-group/data/processed-data/etl-outputs/product_catelog/before_cleansing/2025-04/'
result_path_aws = 's3://bigdata-project-group/data/processed-data/etl-outputs/product_catelog/after_cleansing/2025-04/'

# Basic stop words for text cleaning
STOP_WORDS = {
    'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by',
    'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did',
    'will', 'would', 'could', 'should', 'may', 'might', 'must', 'can', 'this', 'that', 'these',
    'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they', 'me', 'him', 'her', 'us', 'them',
    'my', 'your', 'his', 'her', 'its', 'our', 'their', 'am', 'from', 'up', 'about', 'into',
    'through', 'during', 'before', 'after', 'above', 'below', 'out', 'off', 'over', 'under',
    'again', 'further', 'then', 'once'
}


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


def clean_text_spark(text):
    """
    Text cleaning function optimized for Spark UDF
    Performs basic text cleaning without NLTK dependencies
    """
    if not text or text is None:
        return ""
    
    try:
        text = str(text).strip()
        
        if not text:
            return ""
        
        # Remove HTML tags
        text = re.sub(r'</?[^>]+>', '', text)
        
        # Convert to lowercase
        text = text.lower()
        
        # Remove URLs
        text = re.sub(r'http\S+|www\S+|https\S+', '', text)
        
        # Keep only letters, numbers and spaces
        text = re.sub(r'[^a-z0-9\s]', '', text)
        
        # Replace multiple spaces with single space
        text = re.sub(r'\s+', ' ', text)
        
        # Remove stop words and short words
        words = text.split()
        cleaned_words = [word for word in words if word not in STOP_WORDS and len(word) > 1]
        
        return ' '.join(cleaned_words).strip()
        
    except Exception as e:
        print(f"Error cleaning text: {e}")
        return ""


# Register UDF for text cleaning
clean_text_udf = udf(clean_text_spark, StringType())


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
    Write DataFrame to Parquet using pure Spark operations with retry logic
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
        print("Starting product catalog text cleaning...")
        
        # Get file paths
        file_path, result_path = select_path(execute_platform)
        print(f'Input path: {file_path}')
        print(f'Result path: {result_path}')
        
        # Read parquet files using Spark
        print("Reading parquet files...")
        if file_path.endswith(".parquet") or file_path.endswith(".parquet.gzip"):
            # Single file
            df = spark.read.parquet(file_path)
        else:
            # Directory of parquet files
            df = spark.read.parquet(file_path)
        
        print("Initial data schema:")
        df.printSchema()
        
        initial_count = df.count()
        print(f"Initial record count: {initial_count}")
        
        # Show sample data before cleaning
        print("Sample data before cleaning:")
        df.select("product_name").show(5, truncate=False)
        
        # Apply text cleaning to product_name column
        print("Applying text cleaning...")
        df_cleaned = df.withColumn("clean_name", clean_text_udf(col("product_name")))
        
        # Handle null values in clean_name
        df_cleaned = df_cleaned.withColumn(
            "clean_name", 
            when(col("clean_name").isNull() | (col("clean_name") == ""), "")
            .otherwise(col("clean_name"))
        )
        
        print("Text cleaning completed. Sample cleaned data:")
        df_cleaned.select("product_name", "clean_name").show(5, truncate=False)
        
        # Show statistics
        final_count = df_cleaned.count()
        empty_clean_names = df_cleaned.filter(col("clean_name") == "").count()
        
        print(f"Final record count: {final_count}")
        print(f"Records with empty clean_name: {empty_clean_names}")
        print(f"Records with non-empty clean_name: {final_count - empty_clean_names}")
        
        # Write the result
        print("Writing cleaned data...")
        success = write_parquet_safely_spark(df_cleaned, execute_platform, result_path)
        
        if success:
            print("✅ Product catalog text cleaning completed successfully!")
        else:
            print("❌ Product catalog text cleaning completed but file write may have failed")
            
    except Exception as e:
        print(f"Error in main processing: {e}")
        raise


if __name__ == '__main__':
    sns = boto3.client('sns')
    SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:416191274488:alert-workflow-failed"
    try :
        main()
        job.commit()
    except Exception as error :
        alert_msg = f"ALERT: Workflow failed.Error is {error}"
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=alert_msg
        )