#!/bin/bash

SPARK_HOME=$SPARK_HOME
APP_JAR="/home/rahin/source-code/spark/spark_id_cird_data_transformer/target/spark_ip_cidr_data_transformer-1.0-SNAPSHOT-jar-with-dependencies.jar"
INPUT_PATH="/sandbox/storage/data/ip_cidr_data/dataset/ip_cidr_data_parquet"
OUTPUT_PATH="/sandbox/storage/data/ip_cidr_data/filter_data/spark_extracted_data"
PARTITIONS="2"

$SPARK_HOME/bin/spark-submit \
    --master spark://dev-server01:7077 \
    --deploy-mode client \
    --class org.data.transformer.IpCidrPIIDataExtractor \
    --name IpCidrPIIDataExtractor \
    --driver-memory 2G \
    --driver-cores 2 \
    --executor-memory 2G \
    --executor-cores 1 \
    --total-executor-cores 12 \
    $APP_JAR $INPUT_PATH $OUTPUT_PATH $PARTITIONS
