import sys
import requests
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import Row
import boto3

# ===== SET UP LOGGING =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ===== GET JOB NAME =====
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# ===== SPARK / GLUE CONTEXT =====
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ===== INIT JOB =====
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger.info(f"Started Glue job: {args['JOB_NAME']}")

# ===== CONFIG PATH =====
INPUT_PATH = "s3://bigdata-project-group/data/processed-data/etl-outputs/product_mapping/2025-05/"
OUTPUT_PATH = "s3://bigdata-project-group/data/processed-data/model-outputs/sentiment-analysis/socialmedia/2025-05/"
SENTIMENT_ENDPOINT = "http://ec2-3-88-85-229.compute-1.amazonaws.com/predict"

# ===== SENTIMENT FUNCTION =====
def get_sentiment(text):
    try:
        response = requests.post(SENTIMENT_ENDPOINT, json={"text": text}, timeout=10)
        if response.status_code == 200:
            sentiment = response.json().get("sentiment", "UNKNOWN")
            logger.debug(f"Sentiment received: {sentiment}")
            return sentiment
        else:
            logger.warning(f"Non-200 response from sentiment API: {response.status_code}")
            return f"ERROR_{response.status_code}"
    except Exception as e:
        logger.error(f"Exception during sentiment API call: {str(e)}")
        return f"ERROR_{str(e)}"

# ===== MAIN WORKFLOW =====
def main():
    logger.info("Reading input data...")
    df = spark.read.parquet(INPUT_PATH).limit(100)
    logger.info(f"Loaded {df.count()} records from {INPUT_PATH}")

    logger.info("Limiting to 100 rows for budget safety...")
    df = df.limit(100)

    logger.info("Collecting rows for API call...")
    rows = df.collect()

    logger.info("Calling sentiment API for each row...")
    result_rows = []
    for i, row in enumerate(rows):
        sentiment = get_sentiment(row['review_text'])
        new_row = Row(**row.asDict(), sentiment=sentiment)
        result_rows.append(new_row)
        if i % 10 == 0:
            logger.info(f"Processed {i + 1} records")

    logger.info("Creating new DataFrame with sentiment column...")
    result_df = spark.createDataFrame(result_rows)

    logger.info("Writing output as a single Parquet file...")
    result_df.coalesce(1).write.mode("overwrite").parquet(OUTPUT_PATH)
    logger.info("Write complete")

# ===== EXECUTION WITH ALERTING =====
if __name__ == "__main__":
    sns = boto3.client('sns')
    SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:851725315772:alert-workflow-failed"
    try:
        main()
        job.commit()
        logger.info("Glue job completed successfully")
    except Exception as error:
        alert_msg = f"ALERT -- JOB_NAME: 3_sentiment-analysis-socialmedia -- Workflow failed. Error: {error}"
        logger.error(alert_msg)
        sns.publish(TopicArn=SNS_TOPIC_ARN, Message=alert_msg)
        raise