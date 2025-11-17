import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

AWS_ACCESS_KEY_ID = "minioadmin"
AWS_SECRET_ACCESS_KEY = "minioadmin"
MINIO_ENDPOINT_URL = "http://minio:9000"

spark = SparkSession.builder \
    .appName("KafkaToS3DeltaLake") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.cores", "2") \
    .config("spark.cores.max", "2") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT_URL) \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .enableHiveSupport() \
    .getOrCreate()

print("Spark Session Created and connected to cluster.")

# 2. Schema of Kafka JSON 'value'
json_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True), # Read it as a string
    StructField("page", StringType(), True),
    StructField("value", DoubleType(), True)
])

# 3.Define Kafka Source (Read Stream) 
# "startingOffsets" 
# latest-> Fetch data after start
# earliest -> Fetch data from earliest point
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "user_events") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

print("Kafka stream reader created.")

# 4. Transform
# a. Parse JSON
# Parse 'value' as json string and make an alias 'data', then select all data.*
parsed_df = kafka_df \
    .select(from_json(col("value").cast("string"), json_schema).alias("data")) \
    .select("data.*")

# b. Transform timestamp and create minutes-level partition
parsed_df_with_partition = parsed_df \
    .withColumn("event_timestamp", expr("to_timestamp(timestamp)")) \
    .withColumn("event_minute", date_format(col("event_timestamp"), "yyyy-MM-dd-HH-mm")) \
    .drop("timestamp") # Discard original timestamp

# 5. Write Stream to S3
S3_BUCKET = "prod-data"

delta_stream_query = parsed_df_with_partition.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/user_events_delta") \
    .partitionBy("event_minute") \
    .trigger(processingTime="15 seconds") \
    .start(f"s3a://{S3_BUCKET}/data/user_events_delta") 

print(f"Writing stream to Delta table on S3 (s3a://{S3_BUCKET}/data/user_events_delta)...")
print(f"Partitioning by 'event_minute'.")
print(f"Using checkpoint location (s3a://{S3_BUCKET}/checkpoints/user_events_delta)...")

# 6. Keep it running
delta_stream_query.awaitTermination()