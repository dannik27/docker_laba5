docker stack deploy -c mystack.yml mystack

docker cp target/spark-nasa-1.0.jar hadoop-master:/root/app.jar
docker cp init_master.sh hadoop-master:/root/init.sh
