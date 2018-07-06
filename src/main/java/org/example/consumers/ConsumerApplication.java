package org.example.consumers;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.example.messages.Message;
import org.example.serds.MessageSerde;

public class ConsumerApplication {

    public static void main(String[] args) {

        Serde<Message> messageSerde = new MessageSerde();
        Serde<Long> keySerde = Serdes.Long();

        String topic = "test-topic";

        Consumer vanilaConsumer = new VanillaConsumer<>(topic, 2L, keySerde, messageSerde,
                record -> System.out.println("[" + topic + "-" + 2L + "][vanila] Received message: (" +
                        record.key() + ", " + record.value() + ") at offset " + record.offset()));

        Consumer streamConsumer = new StreamConsumer<>(topic, 1L, keySerde, messageSerde,
                (key, value) -> System.out.println("[" + topic + "-" + 1L + "][stream] " +
                "Received message: " + key + " - " + value));

        vanilaConsumer.start();
        streamConsumer.start();
    }
    
}
