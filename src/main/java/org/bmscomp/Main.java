package org.bmscomp;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {

    public static void main(String[] args) {

        KafkaConsumer<String, String> consumer = createConsumer();
        consumer.subscribe(Collections.singleton("wikimedia"));

        try (consumer) {
            for (;;) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.value());
                }
            }
        }
    }


    public static KafkaConsumer<String, String> createConsumer() {
        var bootstrapServers = "localhost:9092";
        var groupId = "consumer-opensearch";
        // var topicName = "kafka-demo";

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<>(props);
    }


}