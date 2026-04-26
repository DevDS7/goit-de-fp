import os
import sys
import subprocess

os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from kafka_configs import kafka_config

spark = SparkSession.builder \
    .appName("ReadOutputKafkaTopic") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
    ) \
    .getOrCreate()

bootstrap_servers = kafka_config["bootstrap_servers"]

if isinstance(bootstrap_servers, list):
    bootstrap_servers = ",".join(bootstrap_servers)

output_topic = "athlete_aggregated_results_ds"

df_kafka = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", output_topic) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .option("kafka.security.protocol", kafka_config["security_protocol"]) \
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{kafka_config["username"]}" '
        f'password="{kafka_config["password"]}";'
    ) \
    .load()

df_kafka.selectExpr("CAST(value AS STRING) AS message") \
    .show(20, truncate=False)

spark.stop()