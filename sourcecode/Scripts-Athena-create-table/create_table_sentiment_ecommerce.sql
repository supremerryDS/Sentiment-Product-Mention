CREATE EXTERNAL TABLE IF NOT EXISTS `sentiment-analysis`.sentiment_analysis_ecommerce (
  product_id STRING,
  product_name STRING,
  rating DOUBLE,
  review_text STRING,
  sentiment_humanlabels STRING,
  platform STRING,
  clean_product_name STRING,
  clean_text_review STRING,
  sentiment_api STRING
)
STORED AS PARQUET
LOCATION 's3://bigdata-project-group/data/processed-data/model-outputs/sentiment-analysis/e-commerce/2025-05/';
