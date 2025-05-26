import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import re
from urllib.parse import urlparse
import boto3
import time
from pyspark.sql.functions import col, when, isnan, isnull
from pyspark.sql.types import *

# Define common English stopwords
STOPWORDS = {
    'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from', 'has', 'he',
    'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the', 'to', 'was', 'will', 'with',
    'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours',
    'yourself', 'yourselves', 'him', 'his', 'himself', 'she', 'her', 'hers', 'herself',
    'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom',
    'this', 'these', 'those', 'am', 'been', 'being', 'have', 'had', 'having', 'do',
    'does', 'did', 'doing', 'would', 'should', 'could', 'ought', 'can', 'might', 'must',
    'shall', 'may', 'need', 'dare', 'if', 'or', 'because', 'as', 'until', 'while',
    'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after',
    'above', 'below', 'up', 'down', 'out', 'off', 'over', 'under', 'again', 'further',
    'then', 'once'
}

# Initialize Glue components
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define platform: 'local' or 'aws'
execute_platform = 'aws'

# Local paths
LOCAL_PRODUCT_PATH = r"/tmp/product_catelog_after_cleansing.parquet.gzip"
LOCAL_REVIEW_PATH = r"/tmp/customer_mention_texts_after_cleansing.parquet.gzip"
LOCAL_RESULT_PATH = r"/tmp/customer_mention_mapping_products.parquet.gzip"

# S3 paths
S3_PRODUCT_PATH = 's3://bigdata-project-group/data/processed-data/etl-outputs/product_catelog/after_cleansing/2025-04/'
S3_REVIEW_PATH = "s3://bigdata-project-group/data/processed-data/etl-outputs/social_media_posts/after_cleansing/2025-05/"
S3_RESULT_PATH = "s3://bigdata-project-group/data/processed-data/etl-outputs/product_mapping/2025-05/"

##############################################
def select_path(platform: str):
    """Select correct file paths based on execution platform."""
    if platform == 'local':
        return LOCAL_PRODUCT_PATH, LOCAL_REVIEW_PATH, LOCAL_RESULT_PATH
    elif platform == 'aws':
        return S3_PRODUCT_PATH, S3_REVIEW_PATH, S3_RESULT_PATH
    else:
        raise ValueError(">> Invalid platform specified. Choose 'local' or 'aws'.")

##############################################

def simple_tokenize(text):
    """Simple tokenization without external libraries"""
    if not text:
        return []
    tokens = re.findall(r'\b\w+\b', text.lower())
    return tokens

def preprocess_text(text):
    """Clean and preprocess text data"""
    if not text or pd.isna(text):
        return ""
    
    # Convert to lowercase
    text = str(text).lower()
    
    # Remove special characters (but keep spaces, alphabets and numbers)
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    
    # Simple tokenization
    tokens = simple_tokenize(text)
    
    # Remove stopwords and short words
    tokens = [word.strip() for word in tokens if word and word not in STOPWORDS and len(word) > 2]
    
    # Join tokens back
    return ' '.join(tokens)

def calculate_similarity(reviews, products):
    """Calculate similarity between reviews and products"""
    # Preprocess all texts
    preprocessed_reviews = [preprocess_text(review) for review in reviews]
    preprocessed_products = [preprocess_text(product) for product in products]
    
    # Filter out empty strings
    preprocessed_reviews = [r for r in preprocessed_reviews if r.strip()]
    preprocessed_products = [p for p in preprocessed_products if p.strip()]
    
    # Combine all texts for vectorization
    all_texts = preprocessed_reviews + preprocessed_products
    
    # Create TF-IDF vectorizer
    vectorizer = TfidfVectorizer(max_features=10000, stop_words='english')
    tfidf_matrix = vectorizer.fit_transform(all_texts)
    
    # Split the matrix back to reviews and products
    review_vectors = tfidf_matrix[:len(preprocessed_reviews)]
    product_vectors = tfidf_matrix[len(preprocessed_reviews):]
    
    # Calculate cosine similarity
    similarity_matrix = cosine_similarity(review_vectors, product_vectors)
    
    return similarity_matrix

def find_most_similar_products(similarity_matrix, reviews, product_df, top_n=1):
    """Find the most similar products for each review and map to product_id"""
    results = []
    
    for i, review in enumerate(reviews):
        if i >= len(similarity_matrix):
            break
            
        # Get similarity scores for this review
        similarity_scores = similarity_matrix[i]
        
        # Get indices of top N similar products
        top_indices = similarity_scores.argsort()[-top_n:][::-1]
        
        # Get product details
        top_products = []
        for idx in top_indices:
            if idx < len(product_df):
                product_name = product_df.iloc[idx]['clean_name']
                product_id = product_df.iloc[idx]['product_id']
                similarity_score = float(similarity_scores[idx])
                top_products.append({
                    'product_id': product_id,
                    'product_name': product_name,
                    'similarity_score': similarity_score
                })
        
        results.append({
            'review': review,
            'top_products': top_products
        })
    
    return results

def make_path_string(result_path):
    if result_path.startswith("s3://"):
        # Do nothing for S3 paths
        print(f"S3 path detected: {result_path} — skipping directory creation.")
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
        raise TypeError("Input must be a pandas DataFrame")

    if execute_platform == 'aws':
        try:
            parsed = urlparse(result_path)
            bucket_name = parsed.netloc
            key = parsed.path.lstrip('/')

            print("---------------------- Writing data to S3... ----------------------")

            # Convert pandas DataFrame to Spark DataFrame then use Glue
            spark_df = spark.createDataFrame(df)
            dynamic_frame = DynamicFrame.fromDF(spark_df, glueContext, "results")
            
            # Write using Glue
            glueContext.write_dynamic_frame.from_options(
                frame=dynamic_frame,
                connection_type="s3",
                connection_options={"path": result_path, "compression": "gzip"},
                format="parquet"
            )

            print(f"---------------------- Saved file to S3: {result_path} ----------------------")

            # Retry to confirm existence
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

        except Exception as e:
            print(f"Failed to write to S3. Error: {e}")
            return False

    else:  # Local
        try:
            print("---------------------- Writing data locally... ----------------------")
            
            # Convert pandas DataFrame to Spark DataFrame for consistent processing
            spark_df = spark.createDataFrame(df)
            
            # Write using Spark's parquet writer
            spark_df.coalesce(1).write.mode("overwrite").option("compression", "gzip").parquet(result_path)
            
            print(f"---------------------- Saved file locally: {result_path} ----------------------")
            return True

        except Exception as e:
            print(f"Failed to write locally. Error: {e}")
            return False

def main():
    product_path, review_path, output_path = select_path(execute_platform)

    if execute_platform == 'aws':
        # Read data using Glue DynamicFrames for S3
        print("Reading product data from S3...")
        product_dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [product_path]},
            format="parquet"
        )
        
        print("Reading review data from S3...")
        review_dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="s3", 
            connection_options={"paths": [review_path]},
            format="parquet"
        )
        
        # Convert to Spark DataFrames
        spark_products = product_dynamic_frame.toDF()
        spark_reviews = review_dynamic_frame.toDF()
    
    else:  # Local
        # Read data using Spark for local files
        print("Reading product data locally...")
        spark_products = spark.read.parquet(product_path)
        
        print("Reading review data locally...")
        spark_reviews = spark.read.parquet(review_path)
    
    # Clean data using Spark operations
    spark_products = spark_products.filter(
        col("clean_name").isNotNull() & 
        col("product_id").isNotNull() &
        (col("clean_name") != "") &
        (col("product_id") != "")
    )
    
    spark_reviews = spark_reviews.filter(
        col("clean_text").isNotNull() & 
        (col("clean_text") != "")
    )
    
    # Convert to pandas for ML processing (necessary for sklearn)
    df_products = spark_products.toPandas()
    df_reviews = spark_reviews.toPandas()
    
    print(f"Loaded {len(df_products)} products and {len(df_reviews)} reviews")

    # Calculate similarity using the existing functions
    similarity_matrix = calculate_similarity(df_reviews['clean_text'].tolist(), df_products['clean_name'].tolist())
    results = find_most_similar_products(similarity_matrix, df_reviews['clean_text'].tolist(), df_products)

    # Create mapping results
    mapping_results = [
        {
            'review_id': i+1,
            'review_text': result['review'],
            'product_id': product['product_id'],
            'product_name': product['product_name'],
            'similarity_score': product['similarity_score']
        }
        for i, result in enumerate(results)
        for product in result['top_products']
    ]

    mapping_df = pd.DataFrame(mapping_results)
    make_path_string(output_path)
    write_parquet_safely(mapping_df, execute_platform, output_path)

if __name__ == "__main__":
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