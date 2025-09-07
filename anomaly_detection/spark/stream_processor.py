from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count, sum, when, stddev
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)


def main():
    spark = SparkSession.builder.appName("WebLogProcessor").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9093")
        .option("subscribe", "web_logs")
        .load()
    )

    # Data from Kafka is a binary value column
    lodg_df_string = kafka_df.selectExpr("CAST(value AS STRING)")

    log_schema = StructType(
        [
            StructField("timestamp", StringType(), True),
            StructField("ip", StringType(), True),
            StructField("method", StringType(), True),
            StructField("url", StringType(), True),
            StructField("status_code", IntegerType(), True),
            StructField("response_time_ms", IntegerType(), True),
        ]
    )

    # parse JSON
    parsed_logs_df = lodg_df_string.withColumn(
        "log_data", from_json(col("value"), log_schema)
    ).select("log_data.*")

    # Convert timestamp string to actual timestamp type
    logs_with_timestamp = parsed_logs_df.withColumn(
        "timestamp", col("timestamp").cast(TimestampType())
    )

    stats_df = logs_with_timestamp.groupBy(window(col("timestamp"), "1 minute")).agg(
        count("*").alias("request_count"),
        avg("response_time_ms").alias("avg_response_time"),
        sum(when(col("status_code") >= 400, 1).otherwise(0)).alias("error_count"),
        stddev("response_time_ms").alias("stddev_response_time"),
    )

    # TODO: ML model to detect anomalies
    anomaly_df = stats_df.withColumn(
        "is_anomaly",
        (col("error_count") > 5)
        | ((col("avg_response_time") > 900) & (col("stddev_response_time") > 450)),
    )

    # Simple analytics - total requests, avg response time, error count
    # Aggregations over 1-minute tumbling-window
    # analytics_df = parsed_logs_df.groupBy(
    #     # Group data into 1-minute windows
    #     window(col("timestamp"), "1 minute")
    # ).agg(
    #     # Calculate metrics for each window
    #     count("*").alias("total_requests"),
    #     avg("response_time_ms").alias("avg_response_time_ms"),
    #     sum(when(col("status_code") >= 500, 1).otherwise(0)).alias("error_count"),
    # )

    query = (
        anomaly_df.select(
            col("window.start"),
            "request_count",
            "avg_response_time",
            "error_count",
            "stddev_response_time",
            "is_anomaly",
        )
        .writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
