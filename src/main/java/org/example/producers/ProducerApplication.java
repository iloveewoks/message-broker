package org.example.producers;

public class ProducerApplication {

    public static void main(String[] args) {
        boolean isAsync = true;
        Producer producerThread = new Producer("test-topic", isAsync);
        producerThread.start();

        try {
            Thread.sleep(10_000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        producerThread.shutdown();
    }
    
}
