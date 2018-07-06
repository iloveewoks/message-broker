package org.example.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serde;

import java.util.Collections;
import java.util.Properties;

public class VanilaConsumer<K, V> extends Consumer {
    private final KafkaConsumer<K, V> consumer;
    private final String TOPIC;
    private final Long MESSAGE_ID;

    public static final String KAFKA_SERVER_URL = "localhost";
    public static final int KAFKA_SERVER_PORT = 9092;
    public static final String CLIENT_ID = "VanilaConsumer";

    public VanilaConsumer(String topic, Long messageId, Serde<K> keySerde, Serde<V> valueSerde) {
        super("KafkaConsumerExample", false);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");

        consumer = new KafkaConsumer<>(props, keySerde.deserializer(), valueSerde.deserializer());
        this.TOPIC = topic + "-" + messageId;
        this.MESSAGE_ID = messageId;
    }

    @Override
    public void doWork() {
        consumer.subscribe(Collections.singletonList(this.TOPIC));

        consumer.poll(1000)
                .forEach(record -> System.out.println("[" + TOPIC + "][vanila] Received message: (" + record.key() + ", " +
                                                    record.value() + ") at offset " + record.offset()));
    }
}
