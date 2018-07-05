package org.example.producers;

import kafka.utils.ShutdownableThread;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.example.messages.Message;
import org.example.serds.JsonPOJODeserializer;
import org.example.serds.JsonPOJOSerializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ProducerApplication {

    public static void main(String[] args) {

        Map<String, Object> serdeProps = new HashMap<>();

        final Serializer<Message<Long, String>> MessageSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", Message.class);
        MessageSerializer.configure(serdeProps, false);

        final Deserializer<Message<Long, String>> MessageDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", Message.class);
        MessageDeserializer.configure(serdeProps, false);

        Serde<Message<Long, String>> messageSerde = Serdes.serdeFrom(MessageSerializer, MessageDeserializer);
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
