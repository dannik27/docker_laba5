
# Запуск в докер контейнере

cluster-start-swarm.bat - поднимает два сервиса: hadoop-master и hadoop-slave, ожидает пока контейнеры поднимутся и подключается к 
hadoop-master.

### Запуск hadoop и выполнение задачи. Скрипты лежат в каталоге /root

init_master - запускает hadoop и spark и копирует необходимые данные на hdfs.

run_over_yarn - запуск задачи в yarn-режиме

run_standalone - запуск в standalone режиме

get_result - копирует результаты с hdfs в локальное хранилище, очищает hdfs

