package org.example.serds;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.example.messages.Message;

import java.util.HashMap;
import java.util.Map;

public class MessageSerd implements Serde<Message> {

    final private JsonPOJOSerializer<Message> serializer;
    final private JsonPOJODeserializer<Message> deserializer;

    {
        Map<String, Object> serdeProps = new HashMap<>();

        serializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", Message.class);
        serializer.configure(serdeProps, false);

        deserializer = new JsonPOJODeserializer<>();
        deserializer.configure(serdeProps, false);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public Serializer<Message> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Message> deserializer() {
        return deserializer;
    }
}
