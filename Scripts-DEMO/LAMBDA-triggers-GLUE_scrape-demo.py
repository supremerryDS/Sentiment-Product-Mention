import boto3
import os
from urllib.parse import urlparse

s3 = boto3.client('s3')

def lambda_handler(event, context):
    base_dir = "/var/task/data"  # <-- reference data folder

    # Local files (in deployment ZIP under /data/)
    files = [
        ("amazon_reviews.csv", f"{base_dir}/amazon_reviews.csv"),
        ("ebay_reviews.csv", f"{base_dir}/ebay_reviews.csv"),
        ("product_reviews_final.json", f"{base_dir}/product_reviews_final.json")
    ]

    # Corresponding S3 destination paths
    s3_paths = [
        "s3://bigdata-project-group/data/raw-data/ecommerce_reviews/amazon/2025-05/amazon_reviews.csv",
        "s3://bigdata-project-group/data/raw-data/ecommerce_reviews/ebay/2025-05/ebay_reviews.csv",
        "s3://bigdata-project-group/data/raw-data/social_media_posts/2025-05/product_reviews_final.json"
    ]

    for (filename, local_path), s3_uri in zip(files, s3_paths):
        try:
            parsed = urlparse(s3_uri)
            bucket = parsed.netloc
            key = parsed.path.lstrip('/')

            # Upload file to S3
            with open(local_path, 'rb') as f:
                s3.upload_fileobj(f, bucket, key)
                print(f"Uploaded {filename} → s3://{bucket}/{key}")
        except Exception as e:
            print(f"Failed to upload {filename}: {str(e)}")

    glue = boto3.client('glue')
    glue_start = glue.start_workflow_run(
        Name='sentiment-product-mention'
    )
    return {
        "statusCode": 200,
        "body": f"Uploaded {len(files)} files to S3 successfully"
    }
