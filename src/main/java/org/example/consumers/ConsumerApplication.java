package org.example.consumers;

public class ConsumerApplication {

    public static void main(String[] args) {
        String topic = "test-topic";
        Consumer vanilaConsumer = new VanilaConsumer(topic);
        Consumer streamConsumer = new StreamConsumer(topic);

        vanilaConsumer.start();
        streamConsumer.start();
    }
    
}
