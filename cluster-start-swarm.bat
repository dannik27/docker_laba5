@echo off

docker stack deploy -c mystack.yml hadoop

set master=""

:while
if %master%=="" (
    for /f "tokens=1" %%a in ('docker ps ^| findstr hadoop-master') do set master=%%a
    goto :while
)

docker cp target/spark-nasa-1.0.jar %master%:/root/app.jar

docker cp init_master.sh %master%:/root/init_cluster
docker cp get_result.sh %master%:/root/get_result
docker cp run_over_yarn.sh %master%:/root/run_over_yarn
docker cp run_standalone.sh %master%:/root/run_standalone

docker exec -ti %master% bash

