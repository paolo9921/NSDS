package it.polimi.nsds.kafka.eval;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class FilterForwarder {
    private static final String serverAddr = "localhost:9092";

    private static final String inputTopic = "inputTopic";
    private static final String outputTopic = "outputTopic";

    public static final int threshold = 15;

    // TODO add attributes here if needed

    public static void main(String[] args) {
        final String consumerGroupId = args[0];

        // TODO implement here
    }
}