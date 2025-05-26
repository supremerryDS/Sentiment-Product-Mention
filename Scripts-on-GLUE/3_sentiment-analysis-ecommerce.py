import sys
import requests
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

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
OUTPUT_PATH = "s3://bigdata-project-group/data/processed-data/model-outputs/sentiment-analysis/"
SENTIMENT_ENDPOINT = "http://ec2-18-207-245-96.compute-1.amazonaws.com/predict"  # ← เปลี่ยนตามจริง

# ===== LOAD PARQUET FILES =====
df = spark.read.parquet(INPUT_PATH)

# ===== DEFINE UDF TO CALL SENTIMENT API =====
def get_sentiment(text):
    try:
        response = requests.post(
            SENTIMENT_ENDPOINT,
            json={"text": text},
            timeout=10
        )
        if response.status_code == 200:
            return response.json().get("sentiment", "UNKNOWN")
        else:
            return f"ERROR_{response.status_code}"
    except Exception as e:
        return f"ERROR_{str(e)}"

sentiment_udf = udf(get_sentiment, StringType())

# ===== APPLY UDF =====
df_with_sentiment = df.withColumn(
    "sentiment",
    sentiment_udf(col("clean_text_review"))
)

# ===== SELECT FINAL OUTPUT COLUMNS =====
result_df = df_with_sentiment.select(
    col("clean_product_name").alias("product_name"),
    col("clean_text_review").alias("text_review"),
    col("sentiment")
)

# ===== WRITE TO S3 =====
result_df.write.mode("overwrite").parquet(OUTPUT_PATH)

# ===== COMMIT JOB =====
if __name == "__main__" :
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
