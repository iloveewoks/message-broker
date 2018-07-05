package org.example.producers;

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.example.messages.Message;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class Producer extends ShutdownableThread {
    private final KafkaProducer<Long, Message> producer;
    private final String TOPIC;
    private final Long MESSAGE_ID;

    public static final String KAFKA_SERVER_URL = "localhost";
    public static final int KAFKA_SERVER_PORT = 9092;
    public static final String CLIENT_ID = "SampleProducer";

    private final AtomicLong messageNo = new AtomicLong(0);

    public Producer(String topic, Long messageId, Serde keySerde, Serde valueSerde) {
        super("KafkaProducerExample", false);
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        properties.put("client.id", CLIENT_ID);
        properties.put("key.serializer", keySerde.serializer().getClass().getName());
        properties.put("value.serializer", valueSerde.serializer().getClass().getName());

        producer = new KafkaProducer<>(properties);
        this.TOPIC = topic + "-" + messageId;
        this.MESSAGE_ID = messageId;
    }

    @Override
    public void doWork() {
        Message message = new Message(MESSAGE_ID, "Message_" + messageNo.incrementAndGet());

        long startTime = System.currentTimeMillis();
        asyncSend(message, startTime);

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private void asyncSend(Message message, long startTime) {
        producer.send(new ProducerRecord<>(TOPIC, messageNo.get(), message),
                (metadata, exception) -> {
                    long elapsedTime = System.currentTimeMillis() - startTime;

                    if (metadata != null) {
                        System.out.println(message + " sent to partition(" + metadata.partition() + "), " +
                                "offset(" + metadata.offset() + ") " +
                                "in " + elapsedTime + " ms");
                    } else {
                        exception.printStackTrace();
                    }
                });
    }

}
