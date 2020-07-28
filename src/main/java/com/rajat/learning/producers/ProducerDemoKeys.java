package com.rajat.learning.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // Create the Logger
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);


        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i = 0; i < 100; i++){
            String topic = "first_topic";
            String value = "Message Id once again " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            logger.info("Key Number : " + key);

            //Create a producer record
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key,value);

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
            }).get();
        }
        // Flush the data
        producer.flush();

        // Send the data and flush it and close it
        producer.close();
    }
}
