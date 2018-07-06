# Apache Kafka based Messaging System

## Business problem

There is a `Message` class where different `id`'s for messages could be specified. The goal is to process messages according to `id` without relying on conditional structures like `if-else` and `switch`.

## Solution

1. Turn message `id`'s into the Kafka topics.

2. Route messages to specific topic based on it's `id`.

3. Subscribe consumer to desirable `id`-topic and use it to process messages accordingly.


## Possible Downsides

+ Consumers must know ahead available message `id`'s. 

However, since different `id`'s assume different processing behaviour, which means such behaviour should be specified ahead. So the person implementing this behaviour would know about available `id`'s and would have no problem implementing suitable consumer.


## Further Upgrades

+ The main one (as I see now) is generalising `Message` class. Which I believe can be done using Kafka Avro Serdes.

+ Another one is to use Stream based Producer which would provide further convenience in terms of piping and processing of different messaging streams.


## How to Run

1. <details>
    <summary>Launch Kafka</summary>
        
        Inside the Kafka folder execute the following commands:
        
   ```bash
        bin/zookeeper-server-start.sh confizookeeper.properties
        bin/kafka-server-start.sh config/server.properties 
   ```
   </details>

2. Produce some messages using `ProducerApplication` which could be found in `org.example.producers` package.

3. Consume these messages using `ConsumerApplication` which could be found in `org.example.consumers`.

4. There are two Consumer implementations - Streams based and vanilla `KafkaConsumer` based.

   Both implementations require **base topic name**, **desirable message id to process**, **callback processing function**, and **serdes for the *key* and *value* of `ConsumerRecord`** (basic `Long` serde and custom `MessageSerde` are used as an example).
