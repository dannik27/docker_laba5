docker rm -f hadoop-master

docker run -itd --net=hadoop ^
                -p 50070:50070 ^
                -p 8088:8088 ^
                -p 8080:8080 ^
                --name hadoop-master ^
                --hostname hadoop-master ^
                troofy/spark-yarn

docker rm -f hadoop-slave0

docker run -itd --net=hadoop ^
                  --name hadoop-slave0 ^
                  --hostname hadoop-slave0 ^
                  troofy/spark-yarn

docker cp init_master.sh hadoop-master:/root/init.sh

docker exec -it hadoop-master bash
