import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, regexp_replace, lower, trim
from pyspark.sql.types import StringType
import re
import boto3

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

# Define paths
file_path_aws = 's3://bigdata-project-group/data/processed-data/etl-outputs/e-commerce_reviews/before_cleansing/merge_amazon-ebay/2025-05/'
result_path_aws = 's3://bigdata-project-group/data/processed-data/etl-outputs/e-commerce_reviews/after_cleansing/merge_amazon-ebay/2025-05/'

# Basic stop words
stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might', 'must', 'can', 'this', 'that', 'these', 'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they', 'me', 'him', 'her', 'us', 'them', 'my', 'your', 'his', 'her', 'its', 'our', 'their'}

def clean_text_spark(text):
    """Text cleaning function for Spark UDF"""
    if not text:
        return ""
    
    text = str(text).lower()
    
    # Remove HTML tags
    text = re.sub(r'</?[^>]+>', '', text)
    
    # Remove URLs
    text = re.sub(r'http\S+|www\S+|https\S+', '', text)
    
    # Keep only letters, numbers and spaces
    text = re.sub(r'[^a-z0-9\s]', '', text)
    
    # Replace multiple spaces with single space
    text = re.sub(r'\s+', ' ', text)
    
    # Remove stop words
    words = text.split()
    words = [word for word in words if word not in stop_words and len(word) > 1]
    
    return ' '.join(words).strip()

# Register UDF
clean_text_udf = udf(clean_text_spark, StringType())

def main():
    try:
        print("Starting Spark-based data processing...")
        
        # Read parquet files
        df = spark.read.parquet(file_path_aws)
        print(f"Loaded dataframe with {df.count()} rows")
        
        # Apply text cleaning using Spark UDF
        df_cleaned = df.withColumn("clean_product_name", clean_text_udf(col("product_name"))) \
                      .withColumn("clean_text_review", clean_text_udf(col("review_text")))
        
        print("Applied text cleaning transformations")
        
        # Write to S3
        df_cleaned.write \
            .mode("overwrite") \
            .option("compression", "gzip") \
            .parquet(result_path_aws)
        
        print(f"Successfully wrote cleaned data to: {result_path_aws}")
        
    except Exception as e:
        print(f"Error in processing: {e}")
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