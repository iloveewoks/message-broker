package org.example.producers;

import kafka.utils.ShutdownableThread;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.example.messages.Message;
import org.example.serds.MessageSerd;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ProducerApplication {

    public static void main(String[] args) {

        Serde<Message> messageSerde = new MessageSerd();
        Serde keySerde = Serdes.Long();

        String topic = "test-topic";

        List<Producer> producers = Stream.of(1L, 2L)
                .map(id -> new Producer(topic, id, keySerde, messageSerde))
                .collect(Collectors.toList());

        producers.forEach(Thread::start);

        try {
            Thread.sleep(10_000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        producers.forEach(ShutdownableThread::shutdown);
    }
    
}
