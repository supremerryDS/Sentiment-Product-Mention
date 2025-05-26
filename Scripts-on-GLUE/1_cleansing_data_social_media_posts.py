from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when, lit, lower, regexp_replace, split, array_remove, trim
from pyspark.sql.types import StringType, ArrayType
import time
import re
import boto3
import os
from urllib.parse import urlparse

# Initialize Spark Session
def create_spark_session(app_name="TextCleaning", execute_platform='local'):
    if execute_platform == 'local':
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
    else:  # AWS
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
    
    return spark

# Common English stop words (simplified list)
STOP_WORDS = {
    'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours',
    'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', 'her', 'hers',
    'herself', 'it', 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves',
    'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those', 'am', 'is', 'are',
    'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does',
    'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until',
    'while', 'of', 'at', 'by', 'for', 'with', 'through', 'during', 'before', 'after',
    'above', 'below', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again',
    'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all',
    'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor',
    'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will',
    'just', 'don', 'should', 'now'
}

# Configuration
execute_platform = 'aws'  # 'local' or 'aws'
file_path_local = r"C:\Users\panisarak\Desktop\BigDataHW\aws\data\processed-data\etl-outputs\social_media_posts\before_cleansing\2025-05\customer_mention_texts.parquet.gzip"
result_path_local = r"C:\Users\panisarak\Desktop\BigDataHW\aws\data\processed-data\etl-outputs\social_media_posts\after_cleansing\2025-05\customer_mention_texts_after_cleansing.parquet.gzip"
file_path_aws = 's3://bigdata-project-group/data/processed-data/etl-outputs/social_media_posts/before_cleansing/2025-05/'
result_path_aws = 's3://bigdata-project-group/data/processed-data/etl-outputs/social_media_posts/after_cleansing/2025-05/'


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


def clean_text_func(text):
    """Text cleaning function without NLTK dependencies"""
    if not text:
        return ""
    
    try:
        # Remove HTML tags
        text = re.sub(r'</?[^>]+>', '', text)
        
        # Convert to lowercase
        text = text.lower()
        
        # Remove URLs
        text = re.sub(r'http\S+|www\S+|https\S+', '', text)
        
        # Keep only letters, numbers, and spaces
        text = re.sub(r'[^a-z0-9\s]', '', text)
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Simple tokenization and stop word removal
        words = text.split()
        
        # Filter out stop words and single characters
        filtered_words = [word for word in words 
                         if word not in STOP_WORDS 
                         and len(word) > 1
                         and word.strip()]
        
        return ' '.join(filtered_words)
        
    except Exception as e:
        print(f"Error cleaning text: {e}")
        return text if text else ""


def filter_words_func(words):
    """UDF to filter stop words from array of words"""
    if not words:
        return []
    
    return [word for word in words 
            if word not in STOP_WORDS 
            and len(word) > 1 
            and word.strip()]


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


def write_parquet_safely(df, spark, execute_platform, result_path, max_retries=5):
    """Write Spark DataFrame to parquet with retries for S3"""
    try:
        if execute_platform == 'aws':
            print("---------------------- Writing data to S3... ----------------------")
            
            # Write to S3 using Spark
            df.coalesce(1).write \
                .mode("overwrite") \
                .option("compression", "gzip") \
                .parquet(result_path)
            
            print(f"---------------------- Saved file to S3: {result_path} ----------------------")
            
            # Verify file existence
            parsed = urlparse(result_path.replace('s3a://', 's3://'))
            bucket_name = parsed.netloc
            key = parsed.path.lstrip('/')
            
            retry_count = 0
            file_exists = False
            
            while not file_exists and retry_count < max_retries:
                file_exists = check_files_in_s3_path(bucket_name, key)
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
                
        else:  # Local
            print("---------------------- Writing data locally... ----------------------")
            
            # Create directory if it doesn't exist
            dir_path = os.path.dirname(result_path)
            if not os.path.exists(dir_path):
                os.makedirs(dir_path, exist_ok=True)
            
            # Remove existing file if it exists
            if os.path.exists(result_path):
                import shutil
                if os.path.isdir(result_path):
                    shutil.rmtree(result_path)
                else:
                    os.remove(result_path)
            
            # Write parquet file
            df.coalesce(1).write \
                .mode("overwrite") \
                .option("compression", "gzip") \
                .parquet(result_path)
            
            print(f"Successfully wrote file locally: {result_path}")
            return True
            
    except Exception as e:
        print(f"Failed to write parquet file. Error: {e}")
        return False


def clean_text_with_spark_functions(df):
    """Alternative approach using native Spark SQL functions"""
    try:
        # Method using native Spark functions (more efficient for large datasets)
        df_cleaned = df.withColumn("text_lower", lower(col("text"))) \
                      .withColumn("text_no_html", regexp_replace(col("text_lower"), r'</?[^>]+>', '')) \
                      .withColumn("text_no_urls", regexp_replace(col("text_no_html"), r'http\S+|www\S+|https\S+', '')) \
                      .withColumn("text_clean_chars", regexp_replace(col("text_no_urls"), r'[^a-z0-9\s]', '')) \
                      .withColumn("text_normalized", regexp_replace(col("text_clean_chars"), r'\s+', ' ')) \
                      .withColumn("text_trimmed", trim(col("text_normalized"))) \
                      .withColumn("words_array", split(col("text_trimmed"), ' ')) \
                      .withColumn("words_filtered", array_remove(col("words_array"), ''))
        
        # Create UDF for stop word filtering
        filter_words_udf = udf(filter_words_func, ArrayType(StringType()))
        
        df_final = df_cleaned.withColumn("clean_words", filter_words_udf(col("words_filtered"))) \
                            .withColumn("clean_text", 
                                      when(col("clean_words").isNull(), lit(""))
                                      .otherwise(regexp_replace(col("clean_words").cast("string"), r'[\[\],]', '')))
        
        # Clean up the clean_text column format
        df_final = df_final.withColumn("clean_text", 
                                     regexp_replace(col("clean_text"), r'WrappedArray\(|\)', '')) \
                          .withColumn("clean_text", 
                                     regexp_replace(col("clean_text"), r', ', ' '))
        
        # Select only original columns plus clean_text
        original_columns = df.columns
        final_columns = original_columns + ["clean_text"]
        
        return df_final.select(*final_columns)
        
    except Exception as e:
        print(f"Error in Spark functions approach: {e}")
        # Fallback to UDF approach
        return None


def main():
    # Create Spark session
    spark = create_spark_session("TextCleaning", execute_platform)
    
    try:
        # Get file paths
        file_path, result_path = select_path(execute_platform)
        
        print(f"Reading data from: {file_path}")
        
        # Read parquet file(s)
        if file_path.endswith((".parquet", ".parquet.gzip")):
            # Single file
            df = spark.read.parquet(file_path)
        else:
            # Directory or S3 prefix - read all parquet files
            df = spark.read.parquet(file_path + "*.parquet*")
        
        print(f"Initial row count: {df.count()}")
        print("Schema:")
        df.printSchema()
        
        # Try native Spark functions first (more efficient)
        print("Applying text cleaning with Spark native functions...")
        df_cleaned = clean_text_with_spark_functions(df)
        
        # Fallback to UDF if native functions fail
        if df_cleaned is None:
            print("Falling back to UDF approach...")
            clean_text_udf = udf(clean_text_func, StringType())
            df_cleaned = df.withColumn("clean_text", 
                                     when(col("text").isNull(), lit(""))
                                     .otherwise(clean_text_udf(col("text"))))
        
        print(f"Final row count: {df_cleaned.count()}")
        
        # Show sample results
        print("Sample of cleaned data:")
        df_cleaned.select("text", "clean_text").show(5, truncate=False)
        
        # Write results
        print(f"Writing results to: {result_path}")
        success = write_parquet_safely(df_cleaned, spark, execute_platform, result_path)
        
        if success:
            print("---------------------- Process completed successfully ----------------------")
        else:
            print("---------------------- Process completed with errors ----------------------")
            
    except Exception as e:
        print(f"Error in main process: {e}")
        raise
    finally:
        # Stop Spark session
        spark.stop()


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