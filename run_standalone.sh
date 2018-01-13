#!/usr/bin/env bash

spark-submit \
    --class com.dannik.spark_nasa.Starter \
    --master spark://hadoop-master:7077 \
    --total-executor-cores 8 \
    app.jar hadoop-master /nasa/Aug
