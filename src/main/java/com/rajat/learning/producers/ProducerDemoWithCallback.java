package com.rajat.learning.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        // Create the Logger
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);


        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0 ; i < 10; i++){
            //Create a producer record
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>
                    ("first_topic", "Hello World" + Integer.toString(i));

            // Send the data  -  This is sent but not actually received
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // Execute everytime record is successfully Sent or exception is thrown
                    if (e == null){
                        //Successfully Sent
                        logger.info("Recieved Metadata : \n" + "Topic : " + recordMetadata.topic() + "\n"
                                + "Partition :" + recordMetadata.partition() + "\n"
                                + "Offset : " + recordMetadata.offset() + " \n " + "Timestamp : "
                                + recordMetadata.timestamp());
                    }else{
                        logger.error("Error while producing !!", e);
                    }
                }
            });

            // Send the data and flush it
            //producer.flush()
        }

        // Send the data and flush it and close it
        producer.close();
    }
}
