from select import select

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, timestamp_millis, window, sum, last, round
from pyspark.sql.avro.functions import from_avro


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("StockStreamProcessor") \
        .master("local[*]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-avro_2.12:3.5.1") \
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
        .withColumnsRenamed({"p": "price", "s": "symbol", "v": "volume"}) \
        .withColumn("usd_volume", col("price") * col("volume"))  \
        .select("symbol", "price", "volume", "usd_volume", "Timestamp")

    window_agg_df = renamed_df \
        .groupby(window(col("Timestamp"), "1 minute"), col("symbol")) \
        .agg(sum(col("usd_volume")).alias("total_usd_volume"), last(col("price")).alias("last_price"))

    result_df = window_agg_df \
        .select(col("window.start").alias("start_time"),
                col("window.end").alias("end_time"),
                col("symbol"),
                round("total_usd_volume", 2).alias("total_usd_volume"),
                col("last_price"))
    # flatten_df.printSchema()
    # final_df1 = flatten_df \
    #     .groupby(window(col("Timestamp"), "10 seconds"), col("s")) \
    #     .agg(sum(col("usd_volume")).alias("total_usd_volume"))
    #
    # result_df = final_df1.where(expr('s = "AAPL"')) \
    #     .select(col("window.start").alias("start_time"), col("window.end").alias("end_time"), col("s"), col("total_usd_volume"))

    query_stream = result_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("path", "output_data") \
        .option("checkpointLocation", "checkpoint")  \
        .start()

    query_stream.awaitTermination()