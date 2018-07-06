package org.example.consumers;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.example.serds.MessageSerd;

public class ConsumerApplication {

    public static void main(String[] args) {

        Serde messageSerde = new MessageSerd();
        Serde keySerde = Serdes.Long();

        String topic = "test-topic";

//        Consumer vanilaConsumer = new VanilaConsumer(topic, 2L, keySerde, messageSerde);
        Consumer streamConsumer = new StreamConsumer(topic, 1L, keySerde, messageSerde);

//        vanilaConsumer.start();
        streamConsumer.start();

        try {
            Thread.sleep(10_000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
}
