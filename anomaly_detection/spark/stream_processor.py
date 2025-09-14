from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    window,
    avg,
    count,
    sum,
    when,
    stddev,
    min as smin,
    max as smax,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    LongType,
)

import influxdb_client
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

INFLUXDB_URL = "http://influxdb:8086"
INFLUXDB_TOKEN = "my-super-secret-token"
INFLUXDB_ORG = "my-org"
INFLUXDB_BUCKET = "spark-bucket-v2"


def write_to_influxdb(df, epoch_id):
    """
    Called for each micro-batch of data.
    """
    print(f"--- Writing batch {epoch_id} to InfluxDB ---")
    df.show(5)
    row_count = df.count()
    print(f"Batch {epoch_id} has {row_count} records")
    if row_count == 0:
        print("No data in this batch.")
        return

    tmin, tmax = df.select(smin("window.start"), smax("window.end")).first()
    print(f"SPARK EVENT WINDOW: start_min={tmin} to end_max={tmax}")

    # Init  InfluxDB client
    client = influxdb_client.InfluxDBClient(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG,
    )
    write_api = client.write_api(
        write_options=SYNCHRONOUS, write_precision=WritePrecision.S
    )

    # Convert Spark DF to Pandas DF
    pandas_df = df.toPandas()
    print(f"Writing {len(pandas_df)} records to InfluxDB")

    # Create list of "Point" objects to write to InfluxDB
    points = []
    for index, row in pandas_df.iterrows():
        stddev_val = float(
            row["stddev_response_time"] if row["stddev_response_time"] else 0.0
        )
        point = (
            Point("web_analytics")
            .tag("is_anomaly", str(row["is_anomaly"]))  # Tags for filtering
            .field("request_count", int(row["request_count"]))  # Fields for data values
            .field("avg_response_time", float(row["avg_response_time"]))
            .field("error_count", int(row["error_count"]))
            .field("stddev_response_time", stddev_val)
            .time(int(row["start"]), WritePrecision.S)  # timestamp for the data point
        )
        points.append(point)

    # Write the points to InfluxDB
    try:
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points)
        print(f"SUCCESS: Wrote {len(points)} points to InfluxDB to {INFLUXDB_BUCKET}.")
    except Exception as e:
        print(f"ERROR: Could not write to InfluxDB. Exception: {e}")
    finally:
        client.close()


def main():
    spark = (
        SparkSession.builder.appName("RealTimeAnomalyDetector")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9093")
        .option("subscribe", "web_logs_v2")
        # .option("startingOffsets", "latest")  # read only new messages
        .load()
    )

    # Data from Kafka is a binary value column
    lodg_df_string = kafka_df.selectExpr("CAST(value AS STRING)")

    log_schema = StructType(
        [
            StructField("timestamp_ms", LongType(), True),
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
        "timestamp", (col("timestamp_ms") / 1000).cast(TimestampType())
    )

    watermarked = logs_with_timestamp.withWatermark("timestamp", "2 minutes")

    stats_df = watermarked.groupBy(window(col("timestamp"), "1 minute")).agg(
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

    # Extract window start time for InfluxDB timestamp
    final_df = anomaly_df.withColumn("start", col("window.start").cast("long"))

    # Write results to InfluxDB
    query = (
        final_df.writeStream.outputMode("update")
        .foreachBatch(write_to_influxdb)
        .start()
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

    query.awaitTermination()


if __name__ == "__main__":
    main()
