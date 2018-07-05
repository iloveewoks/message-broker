package org.example.producers;

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class Producer extends ShutdownableThread {
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;

    public static final String KAFKA_SERVER_URL = "localhost";
    public static final int KAFKA_SERVER_PORT = 9092;
    public static final String CLIENT_ID = "SampleProducer";

    private static final AtomicInteger messageNo = new AtomicInteger(0);

    public Producer(String topic, Boolean isAsync) {
        super("KafkaProducerExample", false);
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        properties.put("client.id", CLIENT_ID);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(properties);
        this.topic = topic;
        this.isAsync = isAsync;
    }

    @Override
    public void doWork() {
        String message = "Message_" + messageNo.incrementAndGet();

        if (isAsync) {
            long startTime = System.currentTimeMillis();

            asyncSend(message, startTime);
        } else {
            syncSend(message);
        }

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private void asyncSend(String message, long startTime) {
        producer.send(new ProducerRecord<>(topic, messageNo.get(), message),
                (metadata, exception) -> {
                    long elapsedTime = System.currentTimeMillis() - startTime;

                    if (metadata != null) {
                        System.out.println("message(" + messageNo.get() + ", " + message + ") " +
                                "sent to partition(" + metadata.partition() + "), " +
                                "offset(" + metadata.offset() + ") " +
                                "in " + elapsedTime + " ms");
                    } else {
                        exception.printStackTrace();
                    }
                });
    }

    private void syncSend(String message) {
        try {
            producer.send(new ProducerRecord<>(topic, messageNo.get(), message)).get();

            System.out.println("Sent message: (" + messageNo.get() + ", " + message + ")");

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

}
