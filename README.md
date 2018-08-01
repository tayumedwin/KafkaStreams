
## Install Kafka in your local machine: ##

#### $> brew cask install java ####
#### $> brew install kafka ####
#### $> brew services start zookeeper ####
#### $> brew services start kafka ####
#### $> brew services stop kafka ####
#### $> brew services stop zookeeper  ####


## User "kafka.demo" package on this project for basic kstream join ##


## Create topics ##

#### $> kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 12 --topic <name of the topic> ####
##### e.g. $> kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 12 --topic test #####
#### you need 3 topic for this example: ####
##### 1. left #####
##### 2. right #####
##### 3. joined #####
  

## How to see the input from a topic ##

#### kafka-console-consumer --topic <topic name> --from-beginning --bootstrap-server localhost:9092 --property print.key=true #### 
##### e.g. kafka-console-consumer --topic test --from-beginning --bootstrap-server localhost:9092 --property print.key=true #####


