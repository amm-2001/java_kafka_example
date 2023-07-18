package org.example;

public class Main {
    public static void main(String[] args) {

        java_producer PRODUCER_1 =new java_producer("java_topic_example");
PRODUCER_1.start();

java_consumer CONSUMER_1 =new java_consumer("java_topic_example");
CONSUMER_1.start();
    }
}