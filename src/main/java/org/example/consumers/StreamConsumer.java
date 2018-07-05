package org.example.consumers;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamConsumer extends Consumer {
    private final KafkaStreams streams;
    private final CountDownLatch latch;

    public static final String KAFKA_SERVER_URL = "localhost";
    public static final int KAFKA_SERVER_PORT = 9092;
    public static final String CLIENT_ID = "StreamConsumer";

    public StreamConsumer(String topic) {
        super("KafkaStreamConsumerExample", false);
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-consumer");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(topic)
                .foreach((key, value) -> System.out.println("[" + topic + "] " +
                                            "Received message: " + key + " - " + value));

        Topology topology = builder.build();
        streams = new KafkaStreams(topology, props);

        // attach shutdown handler to catch control-c
        latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(topic + "-streams-shutdown-hook") {
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
