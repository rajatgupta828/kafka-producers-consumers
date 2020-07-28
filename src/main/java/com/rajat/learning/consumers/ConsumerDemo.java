package com.rajat.learning.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        String bootStrapServer = "127.0.0.1:9092";
        String groupId = "RajatGroupId2";
        String topic1 = "first_topic";
        
        //Create the logger
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        // Create the properties
        Properties properties = new Properties();

        // Create the config
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //Create the consumer
        KafkaConsumer<String, String> myConsumer = new KafkaConsumer<String, String>(properties);

        //Subscribe the consumer to the right topic(s)
        myConsumer.subscribe(Arrays.asList(topic1));

        // Read/Poll the data
        while(true){
            ConsumerRecords<String,String> recordsRead =  myConsumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> records : recordsRead){
                logger.info("Key :" + records.key() + ", value : " + records.value());
                logger.info(" Partition : " + records.partition() + " Offset : " + records.offset());
            }
        }

    }


}
