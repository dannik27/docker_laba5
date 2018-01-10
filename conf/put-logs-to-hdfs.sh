hdfs dfs -mkdir /nasa

hdfs dfs -put access_log_Aug95 /nasa
hdfs dfs -put access_log_Jul95 /nasa

echo "access_log_Aug95 and access_log_Jul95 were put under '/nasa_logs' hdfs dir"
