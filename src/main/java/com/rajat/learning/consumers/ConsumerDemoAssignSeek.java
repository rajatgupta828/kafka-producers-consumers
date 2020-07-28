package com.rajat.learning.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        new ConsumerDemoAssignSeek().run();
    }

    private ConsumerDemoAssignSeek(){

        }

    private void run () {
        String bootStrapServer = "127.0.0.1:9092";
        String topic1 = "first_topic";
        CountDownLatch latch = new CountDownLatch(1);

        //Create the logger
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

        logger.info("Creating a thread to execute the Consumer");
        ConsumerRunnable myconsumerRunnable = new ConsumerRunnable(bootStrapServer, topic1, latch);


        // Now since the thread is ready to execute, we need to start this thread to be redy for execution
        Thread myConsumerThread = new Thread(myconsumerRunnable);

        myConsumerThread.start();

        // Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught Shutdown Hook");
            myconsumerRunnable.shutDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error("Application is interrupted" + e);
        }finally {
            logger.info("Applicattion is closing");
        }
    }


    // Create the consumers in the threads to make sure, they are running parallely and not executing at once
    public class ConsumerRunnable implements Runnable{

        private CountDownLatch latch;
        private KafkaConsumer<String, String> myConsumer;

        //Create the logger
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
        // This is the constructor of the class, from here the we are going to set up Bootstrap server, Group ID of the
        // consumer , topic names and the countdown latch
        public ConsumerRunnable(String bootStrapServer, String topic1, CountDownLatch latch) {
            this.latch = latch;
            // Create the properties
            Properties properties = new Properties();
            // Create the config
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            //Create the consumer
            myConsumer = new KafkaConsumer<String, String>(properties);
        }
        // We are going to override the run method because we are going to start the thread
        @Override
        public void run() {
            //Assign and seek are usually required to replay the data / use a specific message

            TopicPartition topicPartitionToReadFrom = new TopicPartition("first_topic", 0);
            Long offsetToReadFrom = 15L;

            // Let's see Assign first
            myConsumer.assign(Arrays.asList(topicPartitionToReadFrom));

            //seek
            myConsumer.seek(topicPartitionToReadFrom,offsetToReadFrom);

            try {
                while (true) {
                    ConsumerRecords<String, String> recordsRead = myConsumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> records : recordsRead) {
                        logger.info("Key :" + records.key() + ", value : " + records.value());
                        logger.info(" Partition : " + records.partition() + " Offset : " + records.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Recieved Shutdown Signal");
            } finally {
                // Close the consumer
                myConsumer.close();
                // tell our consumer that we are done with the Consumer
                latch.countDown();
            }
        }
        public void shutDown(){
            // Special method to interrupt the consumer
            // Wake up will throw a exception, that would be WakeUpException
            myConsumer.wakeup();

        }
    }
}
