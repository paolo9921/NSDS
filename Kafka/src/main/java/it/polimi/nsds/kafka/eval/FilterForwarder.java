package it.polimi.nsds.kafka.eval;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class FilterForwarder {
    private static final String serverAddr = "localhost:9092";
    private static final String inputTopic = "inputTopic";
    private static final String outputTopic = "outputTopic";
    public static final int threshold = 15;

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: FilterForwarder <consumer-group-id>");
            System.exit(1);
        }
        final String consumerGroupId = args[0];

        // Configurazione del Consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Per exactly-once semantics

        KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(inputTopic));

        // Configurazione del Producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // Garantisce exactly-once

        KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);

        try {
            while (true) {
                ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, Integer> record : records) {
                    if (record.value() > threshold) {
                        ProducerRecord<String, Integer> outputRecord = new ProducerRecord<>(outputTopic, record.key(), record.value());
                        producer.send(outputRecord);
                    }
                }
                consumer.commitSync();
            }
        } finally {
            consumer.close();
            producer.close();
        }
    }
}
