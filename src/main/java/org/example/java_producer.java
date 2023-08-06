

package org.example;

        import org.apache.avro.Schema;
        import org.apache.avro.generic.GenericData;
        import org.apache.avro.generic.GenericRecord;
        import org.apache.kafka.clients.producer.KafkaProducer;
        import org.apache.kafka.clients.producer.ProducerConfig;
        import org.apache.kafka.clients.producer.ProducerRecord;
        import org.apache.kafka.common.errors.SerializationException;

        import java.util.Properties;


public class java_producer  implements Runnable{
    private Thread thread;
    public String topic;

    public java_producer(String topic) {
        this.topic=topic;
    }

    @Override
    public void run() {


        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");
        KafkaProducer producer = new KafkaProducer(props);



        String userSchema = "{\"type\":\"record\"," +
                "\"name\":\"myrecord\"," +
                "\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);
        GenericRecord avroRecord = new GenericData.Record(schema);

        while (true) {
//TODO
//            PUT YOUR NAME & AGE
            avroRecord.put("name","" );
            avroRecord.put("age",0);
//TODO
//            CHANGE THE KEY IF YOU WANT TO USE IT
            ProducerRecord<Object, Object> record = new ProducerRecord<>(topic, null, avroRecord);

            try {
                producer.send(record, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception == null) {
                                System.out.println("==========================================================================================================================================================");
                                System.out.println("Message sent successfully to partition " + metadata.partition()
                                        + " at offset " + metadata.offset()+ " with key "+record.key()+ " with value "+record.value());
                                System.out.println("==========================================================================================================================================================");
                            } else {
                                System.err.println("Failed to send message: " + exception.getMessage());
                            }
                        }
                    });
//       EVERY 3 SEC THE Producer WILL SEND Message
                Thread.sleep(3000);
            } catch (SerializationException e) {
                // may need to do something with it
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            // When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
            // then close the producer to free its resources.
//            IN THIS CASE PRODUCER SEND FOREVER REMOVE WHILE IF U NEED JUST SINGLE MESSAGE
            finally {
                producer.flush();

                producer.close();
            }

        }
    }
    public void start () {
        System.out.println("Starting Producer");
        if (thread == null) {
            thread = new Thread (this);
            thread.start ();
        }
    }
}




