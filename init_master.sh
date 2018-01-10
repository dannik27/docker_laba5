#!/usr/bin/env bash

start-dfs.sh && start-yarn.sh
hdfs dfs -mkdir /nasa
cp NASA_access_log_Aug95 Aug && cp NASA_access_log_Jul95 Jul
hdfs dfs -put Aug /nasa && hdfs dfs -put Jul /nasa
echo "hadoop cluster is running"
