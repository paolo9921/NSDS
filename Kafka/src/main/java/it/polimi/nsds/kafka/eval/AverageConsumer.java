package it.polimi.nsds.kafka.eval;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class AverageConsumer {
    private static final String serverAddr = "localhost:9092";
    private static final String inputTopic = "inputTopic";

    public static void main(String[] args) {
        final String consumerGroupId = args[0];
        
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(inputTopic));
        
        Map<String, Integer> lastValues = new HashMap<>();
        double average = 0.0;

        try {
            while (true) {
                ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, Integer> record : records) {
                    lastValues.put(record.key(), record.value());
                    
                    double newAverage = lastValues.values().stream().mapToInt(Integer::intValue).average().orElse(0.0);
                    if (newAverage != average) {
                        average = newAverage;
                        System.out.println("Updated average: " + average);
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
}
