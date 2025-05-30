CREATE EXTERNAL TABLE IF NOT EXISTS `sentiment-analysis`.sentiment_analysis_socialmedia (
  review_id BIGINT,
  review_text STRING,
  product_id STRING,
  product_name STRING,
  similarity_score DOUBLE,
  sentiment STRING
)
STORED AS PARQUET
LOCATION 's3://bigdata-project-group/data/processed-data/model-outputs/sentiment-analysis/socialmedia/2025-05/';
