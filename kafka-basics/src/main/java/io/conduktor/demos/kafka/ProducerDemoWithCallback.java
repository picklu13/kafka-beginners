package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Hello");
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        for (int i = 0 ; i < 1; i++){
            String key = "id_" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>("demo1", key, "Hello_World");
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception ==null) {
                        log.info("receieved new metadata \n" +
                                "Topic :" + metadata.topic() +"\n" +
                                "Key : " + record.key() + "\n"+
                                "Partition :" + metadata.partition() + "\n" +
                                "Offset : " + metadata.offset() ) ;
                    }
                }
            });
        }



        producer.flush();
        producer.close();


    }
}
