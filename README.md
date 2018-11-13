### Setup

install node 10.13 (eg: using 'nvm install 10.13')

`# npm install`

Start Kafka container
`# docker run -td --name=kafkaperftest -p 2181:2181 -p 9092:9092 -e ADVERTISED_HOST=localhost  johnnypark/kafka-zookeeper`

### Usage

##### Kafka increase partitions
`# docker exec -ti kafkaperftest /bin/bash`

Inside the container:
`# cd /opt/kafka_2.12-2.0.0/bin/`
`# sh kafka-topics.sh --alter --zookeeper localhost:2181 --topic topic --partitions 8` (or whatever number of partitions)
*If this command fails, run the producer for a second and then run this again.*

##### Consumer(s)
Initiate as many as these as partitions
`# node consumer.js`

##### Producer

node producer msgs_per_cycle cycle_interval_ms=1000

`node producer 10000` for 10.000 messages per sec
