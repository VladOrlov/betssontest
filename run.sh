#!/usr/bin/env bash

INPUT_PATH=${INPUT_PATH}
OUTPUT_PATH=${OUTPUT_PATH}
JOB_TYPE=${JOB_TYPE} #"BALANCE" OR "PROFIT"

SPARK_EXECUTOR_INSTANCES=${SPARK_EXECUTOR_INSTANCES}
SPARK_EXECUTOR_CORES=${SPARK_EXECUTOR_CORES:1}
SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_INSTANCES:2G}
SPARK_DRIVER_CORES=${SPARK_DRIVER_CORES:4}
SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:8G}
SPARK_WRITE_PARTITIONS=${SPARK_WRITE_PARTITIONS:1}

SPARK_USER=${SPARK_USER}
SPARK_HOST=${SPARK_HOST}
SUBMIT_FOLDER=${SUBMIT_FOLDER}


echo "========================================"
echo "source folder: $INPUT_PATH"
echo "destination folder: $OUTPUT_PATH"
echo "document types: $JOB_TYPE"
echo "number of spark executor instances/output files: $SPARK_EXECUTOR_INSTANCES"

# submit job
ssh ${SPARK_USER}@${SPARK_HOST} "
    cd /opt/processing/$SUBMIT_FOLDER;

    spark-submit \
    --class com.betsson.JobRunner \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.executor.instances=$SPARK_EXECUTOR_INSTANCES \
    --conf spark.executor.cores=$SPARK_EXECUTOR_CORES \
    --conf spark.executor.memory=$SPARK_EXECUTOR_MEMORY \
    --conf spark.driver.cores=$SPARK_DRIVER_CORES \
    --conf spark.driver.memory=$SPARK_DRIVER_MEMORY \
    --conf 'spark.executor.extraJavaOptions=-Dfile.encoding=UTF-8' \
    --conf 'spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8' \
    ./bettson-1.0-SNAPSHOT.jar --input $INPUT_PATH --output $OUTPUT_PATH \
    --job-type $JOB_TYPE --executors $SPARK_EXECUTOR_INSTANCES --write-partitions $SPARK_WRITE_PARTITIONS;
"
