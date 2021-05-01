package com.kafka.testing;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

public class Producer {
    public static void main(String[] args) throws IOException {

            produce(args[0], args[1], args[2]);

    }
    public static void produce(String host, String jks_store, String privateKey){
        Properties props = new Properties();
        props.put("bootstrap.servers", host);
        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location", jks_store);
        props.put("ssl.truststore.password", "secret");
        props.put("ssl.keystore.type", "PKCS12");
        props.put("ssl.keystore.location", privateKey);
        props.put("ssl.keystore.password", "secret");
        props.put("ssl.key.password", "secret");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>("hello_world", "test-sample-message1"));
        }

        producer.close();
    }
}