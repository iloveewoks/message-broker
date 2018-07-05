package org.example.consumers;

public class ConsumerApplication {

    public static void main(String[] args) {
        Consumer consumerThread = new Consumer("test-topic");
        consumerThread.start();
    }
    
}
