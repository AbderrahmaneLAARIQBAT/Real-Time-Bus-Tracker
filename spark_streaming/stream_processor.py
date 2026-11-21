from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# إعداد SparkSession
spark = SparkSession.builder \
    .appName("BusGPSStreamProcessor") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# schema ديال البيانات
schema = StructType() \
    .add("bus_id", StringType()) \
    .add("line_id", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("timestamp", StringType()) \
    .add("speed", DoubleType())

# قراءة من Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bus_gps") \
    .option("startingOffsets", "latest") \
    .load()

# تحويل القيمة من بايت إلى JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# كتابة النتائج فـ HDFS (Parquet format)
query = df_parsed.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "hdfs://localhost:9000/user/hadoop/checkpoint_bus_gps") \
    .option("path", "hdfs://localhost:9000/user/hadoop/output/processed_data") \
    .outputMode("append") \
    .start()

query.awaitTermination()
