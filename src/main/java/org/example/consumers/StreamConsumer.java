package org.example.consumers;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamConsumer extends Consumer {
    private final KafkaStreams streams;
    private final CountDownLatch latch;
    private final String TOPIC;
    private final Long MESSAGE_ID;

    public static final String KAFKA_SERVER_URL = "localhost";
    public static final int KAFKA_SERVER_PORT = 9092;

    public StreamConsumer(String topic, Long messageId, Serde keySerde, Serde valueSerde) {
        super("KafkaStreamConsumerExample", false);
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-consumer");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);

        this.TOPIC = topic + "-" + messageId;
        this.MESSAGE_ID = messageId;

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(TOPIC, Consumed.with(keySerde, valueSerde))
                .foreach((key, value) -> System.out.println("[" + TOPIC + "][stream] " +
                                            "Received message: " + key + " - " + value));

        Topology topology = builder.build();
        streams = new KafkaStreams(topology, props);

        // attach shutdown handler to catch control-c
        latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(TOPIC + "-streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });
    }

    @Override
    public void doWork() {
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
