package org.example;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;

import java.util.Arrays;
import java.util.Properties;


public class java_consumer implements Runnable{
    private Thread thread;
    public String topic;



    public java_consumer(String topic){
        this.topic=topic;


    }


    @Override
    public void run() {

        Properties props = new Properties();


        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");

        props.put(ConsumerConfig.GROUP_ID_CONFIG,"group");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"io.confluent.kafka.serializers.KafkaAvroDeserializer");

        props.put("schema.registry.url","http://localhost:8081");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        final Consumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props);







        consumer.subscribe(Arrays.asList(topic));

        try

        {

            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(100);


                for (ConsumerRecord<String, GenericRecord> record : records) {
                    System.out.printf(" offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());

                }
            }
        }
        finally

        {
            consumer.close();
        }

    }
    public void start () {
        System.out.println("Starting Consumer");
        if (thread == null) {
            thread = new Thread (this);
            thread.start ();
        }
    }
}
