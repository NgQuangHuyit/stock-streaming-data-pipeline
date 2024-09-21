import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, timestamp_millis, window, sum, last, round, max, min, expr, count
from pyspark.sql.avro.functions import from_avro
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class StreamingJob:
    KEYSPACE = "stock_market"
    PRICE_TABLE_NAME = "price_tracking"
    VOLUME_TABLE_NAME = "volume_tracking"
    SOURCE_TOPIC = "stock"
    SCHEMA_PATH = "schemas/trades.avsc"

    def __init__(self,
                 cassandra_cluster,
                 cassandra_user,
                 cassandra_password,
                 kafka_bootstrap_servers,
                 minio_access_key,
                 minio_secret_key,
                 minio_endpoint,
                 avro_schema_path):

        self.cassandra_cluster = cassandra_cluster
        self.cassandra_user = cassandra_user
        self.cassandra_password = cassandra_password
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.minio_endpoint = minio_endpoint
        self.logger = logging.getLogger(__name__)
        self.avro_schema_path = avro_schema_path

        self.logger.info("Initializing Spark session")
        self.spark = SparkSession.builder \
            .appName("StockStreamProcessor") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
            .config("spark.cassandra.auth.username", self.cassandra_user) \
            .config("spark.cassandra.auth.password", self.cassandra_password) \
            .config("spark.cassandra.connection.host", self.cassandra_cluster) \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()

        self._load_minio_config()

    def _load_minio_config(self):
        self.logger.info("Configuring MinIO")
        self.spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", self.minio_access_key)
        self.spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", self.minio_secret_key)
        self.spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
                                                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        self.spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        self.spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        self.spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        self.spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        self.spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", self.minio_endpoint)

    def _write_to_cassandra_1(self, df: DataFrame, batch_id):
        self.logger.info(f"Writing micro-batch {batch_id} to Cassandra table {self.PRICE_TABLE_NAME} ")
        try:
            df.write \
                .format("org.apache.spark.sql.cassandra") \
                .options(table=self.PRICE_TABLE_NAME, keyspace=self.KEYSPACE) \
                .mode("append") \
                .save()
            self.logger.info(f"Micro-batch {batch_id} written to Cassandra table {self.PRICE_TABLE_NAME}")
        except Exception as e:
            self.logger.error(f"Error writing micro-batch {batch_id} to Cassandra table {self.PRICE_TABLE_NAME}: {e}")



    def _write_to_cassandra_2(self, df, batch_id):
        self.logger.info(f"Writing micro-batch {batch_id} to Cassandra table {self.VOLUME_TABLE_NAME}")
        try:
            df.write \
                .format("org.apache.spark.sql.cassandra") \
                .options(table=self.VOLUME_TABLE_NAME, keyspace=self.KEYSPACE) \
                .mode("append") \
                .save()
            self.logger.info(f"Micro-batch {batch_id} written to Cassandra table {self.VOLUME_TABLE_NAME}")
        except Exception as e:
            self.logger.error(f"Error writing micro-batch {batch_id} to Cassandra table {self.VOLUME_TABLE_NAME}: {e}")

    def run(self):
        self.logger.info("Starting stream processing job")
        kafka_source = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", "stock") \
            .option("startingOffsets", "earliest") \
            .load()

        avro_schema = open(self.avro_schema_path).read()

        value_df = kafka_source.select(from_avro(col("value"), avro_schema).alias("value")) \
            .select("value.*") \
            .select(explode(col("data")), col("type")) \
            .select("col.*", "type") \
            .withColumn("timestamp", timestamp_millis(col("t"))) \
            .withColumnsRenamed({"p": "price", "s": "symbol", "v": "volume", "cv": "cumulative_volume"}) \
            .withColumn("usd_volume", col("price") * col("volume")) \
            .select("symbol", "timestamp","price", "volume", "usd_volume", "cumulative_volume")

        # Transform and aggregate volume per minute and write stream to cassandra
        volume_per_min_df = value_df \
            .select("symbol", "timestamp", "volume", "usd_volume") \
            .withWatermark("timestamp", "30 seconds") \
            .groupBy(window(col("timestamp"), "1 minutes"), col("symbol")) \
            .agg(
                sum(col("volume")).alias("total_volume"),
                sum(col("usd_volume")).alias("total_usd_volume")
            ) \
            .select(col("symbol"),
                    col("window.start").alias("timestamp"),
                    col("total_volume"),
                    col("total_usd_volume"))
        query_01 = volume_per_min_df.writeStream \
            .outputMode("update") \
            .foreachBatch(self._write_to_cassandra_2) \
            .trigger(processingTime="30 seconds") \
            .option("checkpointLocation", "s3a://checkpoints/query_01") \
            .start()

        # Writestream price tracking df to cassandra
        price_df = value_df \
            .select("symbol", "timestamp", "cumulative_volume", "price")
        query_02 = price_df.writeStream \
            .outputMode("update") \
            .foreachBatch(self._write_to_cassandra_1) \
            .option("checkpointLocation", "s3a://checkpoints/query_02") \
            .start()

        # Transform and aggregate to calculate features for ML model,
        # write stream to MinIO for further training,
        # and write stream to Kafka topic for real-time prediction
        agg_features_df = value_df \
            .withWatermark("timestamp", "10 seconds") \
            .groupby(window(col("timestamp"), "30 seconds", "10 seconds"), col("symbol")) \
            .agg(
                sum(col("usd_volume")).alias("total_usd_volume"),
                sum(col("volume")).alias("total_btc_volume"),
                last(col("price")).alias("close"),
                max(col("price")).alias("high"),
                min(col("price")).alias("low"),
                count(col("price")).alias("num_trades")
            ) \
            .select(col("symbol"),
                    col("window.start").alias("timestamp"),
                    col("total_usd_volume"),
                    col("total_btc_volume"),
                    col("high"),
                    col("low"),
                    col("close"),
                    col("num_trades"))

        kafka_sink_df = agg_features_df.selectExpr("symbol as key",
                                                   """to_json(named_struct(
                                                        "timestamp", timestamp,
                                                        "close", close,
                                                        "high", high,
                                                        "low", low,
                                                        "num_trades", num_trades,
                                                        "total_btc_volume", total_btc_volume,
                                                        "total_usd_volume", total_usd_volume
                                                   )) as value""")
        query_03 = kafka_sink_df.writeStream \
            .outputMode("append") \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("topic", "btc_features") \
            .option("checkpointLocation", "s3a://checkpoints/query_03") \
            .start()

        query_04 = agg_features_df.withColumn("year", expr("year(timestamp)")) \
            .withColumn("month", expr("month(timestamp)")) \
            .withColumn("day", expr("day(timestamp)")) \
            .withColumn("hour", expr("hour(timestamp)")) \
            .writeStream \
            .format("csv") \
            .trigger(processingTime="5 minutes") \
            .partitionBy("year", "month", "day") \
            .option("path", "s3a://featuresstore") \
            .option("checkpointLocation", "s3a://checkpoints/query_04") \
            .start()

        query_01.awaitTermination()
        query_02.awaitTermination()
        query_03.awaitTermination()
        query_04.awaitTermination()



if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--cassandra-cluster", type=str, required=True)
    parser.add_argument("--cassandra-user", type=str, required=True)
    parser.add_argument("--cassandra-password", type=str, required=True)
    parser.add_argument("--kafka-bootstrap-servers", type=str, required=True)
    parser.add_argument("--minio-access-key", type=str, required=True)
    parser.add_argument("--minio-secret-key", type=str, required=True)
    parser.add_argument("--minio-endpoint", type=str, required=True)
    parser.add_argument("--avro-schema-path", type=str, required=True)

    args = parser.parse_args()

    spark_job = StreamingJob(cassandra_cluster=args.cassandra_cluster,
                             cassandra_user=args.cassandra_user,
                             cassandra_password=args.cassandra_password,
                             kafka_bootstrap_servers=args.kafka_bootstrap_servers,
                             minio_access_key=args.minio_access_key,
                             minio_secret_key=args.minio_secret_key,
                             minio_endpoint=args.minio_endpoint,
                             avro_schema_path=args.avro_schema_path)
    spark_job.run()
