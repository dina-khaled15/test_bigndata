#!/bin/bash

exec /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.cores.max=4 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
    /opt/spark-apps/jobs/ais_streaming.py
