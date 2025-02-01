package com.gelindev.kafka_consumer;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Service
public class ConsumerService {

    @Value(value = "${kafka.bootstrap-server}")
    private String bootstrapServer;
    private Properties properties;
    @Value(value = "${kafka.topic}")
    private String topic;
    @Value(value = "${kafka.group-id}")
    private String groupId;
    @Value(value = "${kafka.offset}")
    private String offset;
    private KafkaConsumer<String, String> consumer;

    @PostConstruct
    public void setUp() {
        properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset);

        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(topic));
    }

    @Scheduled(fixedRate = 5000)
    public void process() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        records.forEach(stringStringConsumerRecord -> System.out.println(stringStringConsumerRecord.value()));
    }
}
