#!/usr/bin/env bash

spark-submit \
    --class com.dannik.spark_nasa.Starter \
    --master yarn \
    --deploy-mode client \
    --executor-memory 1G \
    --num-executors 3 \
    app.jar hadoop-master /nasa/Aug
