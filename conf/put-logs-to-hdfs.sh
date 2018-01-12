#!/usr/bin/env bash
start-dfs.sh && start-yarn.sh

spark-submit --class org.apache.spark.examples.SparkPi --master yarn-client /usr/local/spark/examples/jars/spark-examples_2.11-2.2.0.jar  10000

hdfs dfs -mkdir /nasa
cp NASA_access_log_Aug95 Aug && cp NASA_access_log_Jul95 Jul
hdfs dfs -put Aug /nasa && hdfs dfs -put Jul /nasa

spark-submit --class com.dannik.spark_nasa.Starter --master yarn --deploy-mode client --executor-memory 1G --num-executors 3 app.jar hadoop-master /nasa/Aug

start-master.sh && start-slaves.sh
spark-submit --class com.dannik.spark_nasa.Starter --master spark://hadoop-master:7077 --total-executor-cores 8 app.jar hadoop-master /nasa/Aug

hdfs dfs -getmerge /task1 task1.txt
hdfs dfs -rm -r /task*
echo "access_log_Aug95 and access_log_Jul95 were put under '/nasa_logs' hdfs dir"
