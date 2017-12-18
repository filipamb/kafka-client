package com.example.reactivesse;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class SimpleConsumer {
    private final static String TOPIC = "get-tariff";
    private final static String BOOTSTRAP_SERVERS = "10.255.1.42:9092";

    private static KafkaConsumer<Long, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    public static void test() {
        KafkaConsumer<Long, String> consumer = createConsumer();

        //Kafka Consumer subscribes list of topics here.
        consumer.subscribe(Arrays.asList(TOPIC));
        consumer.seekToBeginning(consumer.assignment());

        int i = 0;
        System.out.println("POLLING:");
        ConsumerRecords<Long, String> records = consumer.poll(Long.MAX_VALUE);
        System.out.println("POLLED:"+records.count());
        for (ConsumerRecord<Long, String> record : records) {
            // print the offset,key and value for the consumer records.
            System.out.printf("offset = %d, key = %s, value = %s\n",
                    record.offset(), record.key(), record.value());
        }
        consumer.close();
    }
}
