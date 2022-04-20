package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    public static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Hello");
        String topic = "demo2";
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_app");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topic));
        while (true) {
            log.info("Polling");
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String, String> record : records) {
                log.info("Key : " + record.key() + ", Value :" + record.value());
                log.info("Partition: " + record.partition() + ", Offset : " + record.offset());
            }
        }


    }
}
