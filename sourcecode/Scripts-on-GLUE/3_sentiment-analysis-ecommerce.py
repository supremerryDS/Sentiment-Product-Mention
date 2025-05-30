import sys
import requests
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import boto3

# ===== GET JOB NAME =====
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# ===== SPARK / GLUE CONTEXT =====
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ===== INIT JOB =====
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ===== CONFIG PATH =====
INPUT_PATH = "s3://bigdata-project-group/data/processed-data/etl-outputs/e-commerce_reviews/after_cleansing/merge_amazon-ebay/2025-05/"
OUTPUT_PATH = "s3://bigdata-project-group/data/processed-data/model-outputs/sentiment-analysis/e-commerce/2025-05/"
SENTIMENT_ENDPOINT = "http://ec2-3-88-85-229.compute-1.amazonaws.com/predict"

def get_sentiment(text):
    try:
        response = requests.post(SENTIMENT_ENDPOINT, json={"text": text}, timeout=10)
        if response.status_code == 200:
            return response.json().get("sentiment", "UNKNOWN")
        else:
            return f"ERROR_{response.status_code}"
    except Exception as e:
        return f"ERROR_{str(e)}"

def main():
    # ===== LOAD PARQUET WITH LIMIT =====
    df = spark.read.parquet(INPUT_PATH).limit(100)

    # ===== RENAME COLUMN =====
    df = df.withColumnRenamed("sentiment", "sentiment_humanlabels")

    # ===== COLLECT FOR API CALL (SMALL DATA ONLY) =====
    rows = df.select("product_id", "product_name", "rating", "review_text",
                     "sentiment_humanlabels", "platform", "clean_product_name", "clean_text_review").collect()

    # ===== APPLY SENTIMENT API =====
    results = []
    for row in rows:
        sentiment_api = get_sentiment(row['clean_text_review'])
        results.append((
            row['product_id'],
            row['product_name'],
            row['rating'],
            row['review_text'],
            row['sentiment_humanlabels'],
            row['platform'],
            row['clean_product_name'],
            row['clean_text_review'],
            sentiment_api
        ))

    # ===== CREATE NEW DATAFRAME =====
    result_df = spark.createDataFrame(results, [
        "product_id", "product_name", "rating", "review_text",
        "sentiment_humanlabels", "platform", "clean_product_name",
        "clean_text_review", "sentiment_api"
    ])

    # ===== SAVE TO S3 AS ONE FILE =====
    result_df.coalesce(1).write.mode("overwrite").parquet(OUTPUT_PATH)

if __name__ == "__main__":
    sns = boto3.client('sns')
    SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:851725315772:alert-workflow-failed"
    try:
        main()
        job.commit()
    except Exception as error:
        alert_msg = f"ALERT -- JOB_NAME: 3_sentiment-analysis-ecommerce -- Workflow failed. Error: {error}"
        sns.publish(TopicArn=SNS_TOPIC_ARN, Message=alert_msg)
        raise
