package it.polimi.nsds.kafka.eval;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class AverageConsumer {
    private static final String serverAddr = "localhost:9092";
    private static final String inputTopic = "inputTopic";

    // TODO add attributes here if needed

    public static void main(String[] args) {
        final String consumerGroupId = args[0];

        // TODO implement here
    }
}