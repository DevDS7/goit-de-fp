import os
import sys
import subprocess

os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_json, struct, from_json,
    avg, round, current_timestamp, when,
    lower, trim
)

from jdbc_configs import jdbc_url, jdbc_table_bio, jdbc_table_results, jdbc_user, jdbc_password
from kafka_configs import kafka_config


spark = SparkSession.builder \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
    ) \
    .appName("OlympicFinalProject") \
    .getOrCreate()


# -----------------------------
# 1. Read athlete_bio from MySQL
# -----------------------------

df_bio_raw = spark.read.format("jdbc").options(
    url=jdbc_url,
    driver="com.mysql.cj.jdbc.Driver",
    dbtable=jdbc_table_bio,
    user=jdbc_user,
    password=jdbc_password
).load()

df_bio = df_bio_raw \
    .withColumn("height", col("height").try_cast("double")) \
    .withColumn("weight", col("weight").try_cast("double")) \
    .filter(col("height").isNotNull()) \
    .filter(col("weight").isNotNull())

# -----------------------------
# 2. Read athlete_event_results from MySQL
# -----------------------------

df_results = spark.read.format("jdbc").options(
    url=jdbc_url,
    driver="com.mysql.cj.jdbc.Driver",
    dbtable=jdbc_table_results,
    user=jdbc_user,
    password=jdbc_password
).load()

# -----------------------------
# 3. Write athlete_event_results to Kafka
# -----------------------------

input_topic = "athlete_event_results_ds"

df_results.select(
    col("athlete_id").cast("string").alias("key"),
    to_json(struct("*")).alias("value")
).write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
    .option("topic", input_topic) \
    .option("kafka.security.protocol", kafka_config["security_protocol"]) \
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{kafka_config["username"]}" '
        f'password="{kafka_config["password"]}";'
    ) \
    .save()


# -----------------------------
# 4. Read from Kafka stream
# -----------------------------

df_kafka_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", kafka_config["security_protocol"]) \
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{kafka_config["username"]}" '
        f'password="{kafka_config["password"]}";'
    ) \
    .load()

df_kafka_string = df_kafka_raw.selectExpr("CAST(value AS STRING) as json_value")

results_schema = df_results.schema

df_results_stream = df_kafka_string.select(
    from_json(col("json_value"), results_schema).alias("data")
).select("data.*")


# -----------------------------
# 5. Join with bio data
# -----------------------------

df_bio_clean = df_bio.select(
    "athlete_id",
    "sex",
    "height",
    "weight"
)

df_joined = df_results_stream.join(
    df_bio_clean,
    on="athlete_id",
    how="inner"
)

df_joined = df_joined.withColumn(
    "medal",
    when(
        col("medal").isNull() |
        (lower(trim(col("medal").cast("string"))).isin("nan", "none", "null", "")),
        "No medal"
    ).otherwise(col("medal"))
)


# -----------------------------
# 6. Aggregation
# -----------------------------

df_aggregated = df_joined.groupBy(
    "sport",
    "medal",
    "sex",
    "country_noc"
).agg(
    round(avg("height"), 2).alias("avg_height"),
    round(avg("weight"), 2).alias("avg_weight")
).withColumn(
    "calculation_timestamp",
    current_timestamp()
)


# -----------------------------
# 7. foreachBatch output
# -----------------------------

output_topic = "athlete_aggregated_results_ds"
output_mysql_table = "athlete_aggregated_results_ds_2"


def write_batch_to_outputs(batch_df, batch_id):
    print(f"Processing batch: {batch_id}")

    if batch_df.isEmpty():
        print("Empty batch, skipping...")
        return

    batch_df.select(
        to_json(struct("*")).alias("value")
    ).write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
        .option("topic", output_topic) \
        .option("kafka.security.protocol", kafka_config["security_protocol"]) \
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
        .option(
            "kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{kafka_config["username"]}" '
            f'password="{kafka_config["password"]}";'
        ) \
        .save()

    batch_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", output_mysql_table) \
        .option("user", jdbc_user) \
        .option("password", jdbc_password) \
        .mode("append") \
        .save()


query = df_aggregated.writeStream \
    .foreachBatch(write_batch_to_outputs) \
    .outputMode("update") \
    .option("checkpointLocation", "./checkpoints/athlete_aggregations_v2") \
    .start()

query.awaitTermination()