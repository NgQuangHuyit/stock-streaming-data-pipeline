from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, timestamp_millis, window, sum, last, round, max, min, expr, count
from pyspark.sql.avro.functions import from_avro

def write_to_cassandra(df, batch_id):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="btc_aggregate", keyspace="stock_market") \
        .mode("append") \
        .save()


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("StockStreamProcessor") \
        .master("local[*]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                                       "org.apache.spark:spark-avro_2.12:3.5.1,"
                                       "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1") \
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
        .config("spark.cassandra.auth.username", "cassandra") \
        .config("spark.cassandra.auth.password", "password123") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:19092") \
        .option("subscribe", "stock") \
        .option("startingOffsets", "earliest") \
        .load()

    avro_schema = open("schemas/trades.avsc").read()

    value_df = kafka_df.select(from_avro(col("value"), avro_schema).alias("value")) \
        .select("value.*") \
        .select(explode(col("data")), col("type")) \
        .select("col.*", "type")


    renamed_df = value_df \
        .withColumn("Timestamp", timestamp_millis(col("t"))) \
        .withColumnsRenamed({"p": "price", "s": "symbol", "v": "volume", "cv": "cumulative_volume" }) \
        .withColumn("usd_volume", col("price") * col("volume"))  \
        .select("symbol", "price", "volume", "usd_volume", "Timestamp", "cumulative_volume")

    price_df = renamed_df \
        .select(col("symbol"),
                col("Timestamp").alias("utc_timestamp"),
                col("price").alias("curr_price")) \

    cumulative_volume_df = renamed_df \
        .select(col("symbol"),
                col("Timestamp").alias("utc_timestamp"),
                col("cumulative_volume"))

    pnc_df = renamed_df \
        .select(col("symbol"),
                col("Timestamp").alias("utc_timestamp"),
                col("cumulative_volume"),
                col("price").alias("curr_price"))

    window_agg_df = renamed_df \
        .withWatermark("Timestamp", "1 minutes") \
        .groupby(window(col("Timestamp"), "30 seconds", "10 seconds"), col("symbol")) \
        .agg(
            sum(col("usd_volume")).alias("total_usd_volume"),
            sum(col("volume")).alias("total_btc_volume"),
            last(col("price")).alias("close"),
            max(col("price")).alias("high"),
            min(col("price")).alias("low"),
            count(col("price")).alias("num_trades")
             )



    result_df = window_agg_df \
        .select(col("symbol"),
                col("window.start").alias("utc_start_ts"),
                col("window.end").alias("utc_end_ts"),
                col("total_usd_volume"),
                col("total_btc_volume"),
                col("high"),
                col("low"),
                col("close"),
                col("num_trades")
                )


    query_stream = result_df.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_cassandra) \
        .option("checkpointLocation", "checkpoint")  \
        .start()

    query_stream.awaitTermination()