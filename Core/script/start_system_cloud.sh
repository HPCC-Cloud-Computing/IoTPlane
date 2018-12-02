#!/usr/bin/env bash
#Rabiitmq
#docker run -d --hostname my-rabbit --name rabbitmq -p 15672:15672 -p 5672:5672 rabbitmq:3-management

#Forwarder Cloud to Fog
docker run -itd --name cloud-to-fog -e "BROKER_CLOUD=$BROKER_CLOUD" -e "BROKER_FOG=$BROKER_FOG" haiquan5396/forwarder_cloud_to_fog:1.2

#Registry

docker run -itd --name registry -e "BROKER_CLOUD=$BROKER_CLOUD" -e "HOST_MYSQL=$HOST_MYSQL" -e "MODE=$MODE" haiquan5396/registry:1.2

#Collector

docker run -itd --name collector -e "BROKER_CLOUD=$BROKER_CLOUD" -e "MODE=$MODE" haiquan5396/collector:1.2

#DBCollector

docker run -itd --name db-reader -e "BROKER_CLOUD=$BROKER_CLOUD" -e "HOST_INFLUXDB=$HOST_INFLUXDB" haiquan5396/db-reader:1.2
docker run -itd --name db-write -e "BROKER_CLOUD=$BROKER_CLOUD" -e "HOST_INFLUXDB=$HOST_INFLUXDB" -v /etc/localtime:/etc/localtime:ro haiquan5396/db-writer:1.2

#API

docker run -itd --name api -p 5000:5000 -e "BROKER_CLOUD=$BROKER_CLOUD" haiquan5396/api:1.2
